use super::*;
use crate::input::source::command::SourceCommand;
use evdev::{FFEffectKind, FFReplay, FFTrigger};
use std::sync::mpsc::{channel, Receiver, TryRecvError};

#[derive(Debug, PartialEq)]
enum SourceRequest {
    Upload(FFEffectData),
    Update(i16, FFEffectData),
    Erase(i16),
    Stop,
}

fn mock_source(
    upload_result: Result<i16, &'static str>,
) -> (SourceDeviceClient, Receiver<SourceRequest>) {
    let (tx, mut rx) = mpsc::channel(16);
    let (requests_tx, requests_rx) = channel();
    std::thread::spawn(move || {
        while let Some(command) = rx.blocking_recv() {
            match command {
                SourceCommand::UploadEffect(data, reply) => {
                    requests_tx.send(SourceRequest::Upload(data)).unwrap();
                    let _ = reply.send(upload_result.map_err(|error| error.into()));
                }
                SourceCommand::UpdateEffect(id, data) => {
                    requests_tx.send(SourceRequest::Update(id, data)).unwrap();
                }
                SourceCommand::EraseEffect(id, reply) => {
                    requests_tx.send(SourceRequest::Erase(id)).unwrap();
                    let _ = reply.send(Ok(()));
                }
                SourceCommand::Stop => {
                    requests_tx.send(SourceRequest::Stop).unwrap();
                    break;
                }
                _ => panic!("Unexpected source command: {command:?}"),
            }
        }
    });
    (tx.into(), requests_rx)
}

fn rumble(strong_magnitude: u16) -> FFEffectData {
    FFEffectData {
        direction: 0,
        trigger: FFTrigger {
            button: 0,
            interval: 0,
        },
        replay: FFReplay {
            length: 1000,
            delay: 0,
        },
        kind: FFEffectKind::Rumble {
            strong_magnitude,
            weak_magnitude: 0,
        },
    }
}

fn capabilities(source_id: &str) -> HashMap<String, HashSet<OutputCapability>> {
    HashMap::from([(
        source_id.to_owned(),
        HashSet::from([OutputCapability::ForceFeedbackUpload]),
    )])
}

fn request(rx: &Receiver<SourceRequest>) -> SourceRequest {
    rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap()
}

#[tokio::test]
async fn failed_upload_replies_once_without_reserving_an_effect() {
    let (source, requests) = mock_source(Err("No space left on device"));
    let sources = HashMap::from([("pad".to_owned(), source)]);
    let mut effects = ForceFeedbackEffects::default();
    let (reply, response) = channel();
    let data = rumble(100);
    effects
        .upload_and_reply(0, data, &sources, &capabilities("pad"), &reply)
        .await;
    assert_eq!(response.try_recv(), Ok(None));
    assert_eq!(response.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(request(&requests), SourceRequest::Upload(data));
    assert!(effects.effects.is_empty());
    assert_eq!(effects.available_ids.len(), 64);
}

#[tokio::test]
async fn exhausted_ids_do_not_allocate_physical_effects() {
    let (source, requests) = mock_source(Ok(7));
    let sources = HashMap::from([("pad".to_owned(), source)]);
    let mut effects = ForceFeedbackEffects::default();
    effects.available_ids.clear();
    assert_eq!(
        effects
            .upload_effect(0, rumble(100), &sources, &capabilities("pad"))
            .await,
        None
    );
    assert!(matches!(requests.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn unsupported_sources_are_not_asked_to_upload() {
    let (source, requests) = mock_source(Ok(7));
    let sources = HashMap::from([("keyboard".to_owned(), source)]);
    let mut effects = ForceFeedbackEffects::default();
    assert_eq!(
        effects
            .upload_effect(0, rumble(100), &sources, &HashMap::new())
            .await,
        None
    );
    assert!(matches!(requests.try_recv(), Err(TryRecvError::Empty)));
    assert!(effects.effects.is_empty());
    assert_eq!(effects.available_ids.len(), 64);
}

#[tokio::test]
async fn abandoned_upload_erases_the_physical_effect_and_releases_its_id() {
    let (source, requests) = mock_source(Ok(7));
    let sources = HashMap::from([("pad".to_owned(), source)]);
    let mut effects = ForceFeedbackEffects::default();
    let (reply, response) = channel();
    drop(response);
    let data = rumble(100);
    effects
        .upload_and_reply(0, data, &sources, &capabilities("pad"), &reply)
        .await;
    assert_eq!(request(&requests), SourceRequest::Upload(data));
    assert_eq!(request(&requests), SourceRequest::Erase(7));
    assert!(effects.effects.is_empty());
    assert_eq!(effects.available_ids.len(), 64);
}

#[tokio::test]
async fn source_reconnect_restores_latest_definition_without_restarting_playback() {
    let (source, requests) = mock_source(Ok(7));
    let mut sources = HashMap::from([("old-pad".to_owned(), source)]);
    let mut effects = ForceFeedbackEffects::default();
    let data = rumble(100);
    let id = effects
        .upload_effect(0, data, &sources, &capabilities("old-pad"))
        .await
        .unwrap();
    assert_eq!(request(&requests), SourceRequest::Upload(data));

    effects.remove_source("old-pad");
    sources.clear();
    assert!(!effects.available_ids.contains(&id));
    assert!(effects.effects[&id].source_ids.is_empty());
    let updated = rumble(200);
    assert_eq!(
        effects
            .upload_effect(id, updated, &sources, &HashMap::new())
            .await,
        Some(id)
    );

    let (source, requests) = mock_source(Ok(9));
    effects
        .restore_source("new-pad", &source, &capabilities("new-pad")["new-pad"])
        .await;
    assert_eq!(request(&requests), SourceRequest::Upload(updated));
    assert_eq!(effects.effects[&id].source_ids["new-pad"], 9);
    assert!(matches!(requests.try_recv(), Err(TryRecvError::Empty)));

    sources.insert("new-pad".to_owned(), source);
    let updated = rumble(300);
    assert_eq!(
        effects
            .upload_effect(id, updated, &sources, &capabilities("new-pad"))
            .await,
        Some(id)
    );
    assert_eq!(request(&requests), SourceRequest::Update(9, updated));
    effects.erase_effect(id, &sources).await;
    assert_eq!(request(&requests), SourceRequest::Erase(9));
    assert!(effects.effects.is_empty());
    assert!(effects.available_ids.contains(&id));
}

#[tokio::test]
async fn kernel_effect_id_is_preserved_and_reused_after_erase() {
    let (source, requests) = mock_source(Ok(3));
    let sources = HashMap::from([("pad".to_owned(), source)]);
    let mut effects = ForceFeedbackEffects::default();
    let data = rumble(100);
    assert_eq!(
        effects
            .upload_effect(7, data, &sources, &capabilities("pad"))
            .await,
        Some(7)
    );
    assert_eq!(request(&requests), SourceRequest::Upload(data));
    assert_eq!(effects.effects[&7].source_ids["pad"], 3);
    assert!(effects.available_ids.contains(&0));
    assert!(!effects.available_ids.contains(&7));
    effects.erase_effect(7, &sources).await;
    assert_eq!(request(&requests), SourceRequest::Erase(3));
    assert!(effects.available_ids.contains(&7));
    assert_eq!(
        effects
            .upload_effect(7, data, &sources, &capabilities("pad"))
            .await,
        Some(7)
    );
    assert_eq!(request(&requests), SourceRequest::Upload(data));
    assert_eq!(effects.effects[&7].source_ids["pad"], 3);
}

#[tokio::test]
async fn invalid_or_unavailable_kernel_ids_are_rejected_before_physical_upload() {
    let (source, requests) = mock_source(Ok(3));
    let sources = HashMap::from([("pad".to_owned(), source)]);
    let mut effects = ForceFeedbackEffects::default();
    effects.available_ids.remove(&7);
    for id in [-1, 7, 64] {
        assert_eq!(
            effects
                .upload_effect(id, rumble(100), &sources, &capabilities("pad"))
                .await,
            None
        );
    }
    assert!(matches!(requests.try_recv(), Err(TryRecvError::Empty)));
    assert!(effects.effects.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_restores_retained_effect_after_new_reader_is_scheduled() {
    let (old_source, requests) = mock_source(Ok(3));
    let sources = HashMap::from([("pad".to_owned(), old_source)]);
    let mut effects = ForceFeedbackEffects::default();
    let data = rumble(100);
    assert_eq!(
        effects
            .upload_effect(7, data, &sources, &capabilities("pad"))
            .await,
        Some(7)
    );
    assert_eq!(request(&requests), SourceRequest::Upload(data));
    effects.remove_source("pad");
    drop(sources);

    let (tx, mut rx) = mpsc::channel(1);
    let reader = tokio::spawn(async move {
        let Some(SourceCommand::UploadEffect(data, reply)) = rx.recv().await else {
            panic!("Expected the retained FF definition");
        };
        let _ = reply.send(Ok(9));
        data
    });
    let source = SourceDeviceClient::new(tx);
    effects
        .restore_source("pad", &source, &capabilities("pad")["pad"])
        .await;
    assert_eq!(reader.await.unwrap(), data);
    assert_eq!(effects.effects[&7].source_ids["pad"], 9);
    assert!(!effects.available_ids.contains(&7));
}

#[tokio::test]
async fn force_feedback_only_source_accepts_initial_upload() {
    let (source, requests) = mock_source(Ok(3));
    let sources = HashMap::from([("pad".to_owned(), source)]);
    let capabilities = HashMap::from([(
        "pad".to_owned(),
        HashSet::from([OutputCapability::ForceFeedback]),
    )]);
    let mut effects = ForceFeedbackEffects::default();
    let data = rumble(100);
    assert_eq!(
        effects
            .upload_effect(7, data, &sources, &capabilities)
            .await,
        Some(7)
    );
    assert_eq!(request(&requests), SourceRequest::Upload(data));
    assert_eq!(effects.effects[&7].source_ids["pad"], 3);
}

#[tokio::test]
async fn force_feedback_only_source_restores_retained_definition() {
    let (old, requests) = mock_source(Ok(3));
    let sources = HashMap::from([("pad".to_owned(), old)]);
    let mut effects = ForceFeedbackEffects::default();
    let data = rumble(100);
    assert_eq!(
        effects
            .upload_effect(7, data, &sources, &capabilities("pad"))
            .await,
        Some(7)
    );
    assert_eq!(request(&requests), SourceRequest::Upload(data));
    effects.remove_source("pad");
    drop(sources);

    let (replacement, requests) = mock_source(Ok(9));
    effects
        .restore_source(
            "pad",
            &replacement,
            &HashSet::from([OutputCapability::ForceFeedback]),
        )
        .await;
    assert_eq!(effects.effects[&7].source_ids.get("pad"), Some(&9));
    assert_eq!(request(&requests), SourceRequest::Upload(data));
    assert!(!effects.available_ids.contains(&7));
    assert!(matches!(requests.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn delayed_old_stop_does_not_detach_same_id_replacement() {
    let (old, old_requests) = mock_source(Ok(3));
    let mut sources = HashMap::from([("pad".to_owned(), old.clone())]);
    let mut effects = ForceFeedbackEffects::default();
    let data = rumble(100);
    assert_eq!(
        effects
            .upload_effect(7, data, &sources, &capabilities("pad"))
            .await,
        Some(7)
    );
    assert_eq!(request(&old_requests), SourceRequest::Upload(data));
    assert!(detach_source(&mut sources, &mut effects, "pad", None).await);
    assert_eq!(request(&old_requests), SourceRequest::Stop);

    let (replacement, new_requests) = mock_source(Ok(9));
    sources.insert("pad".to_owned(), replacement.clone());
    effects
        .restore_source("pad", &replacement, &capabilities("pad")["pad"])
        .await;
    assert_eq!(request(&new_requests), SourceRequest::Upload(data));
    assert!(!detach_source(&mut sources, &mut effects, "pad", Some(&old)).await);
    assert!(sources.contains_key("pad"));
    assert_eq!(effects.effects[&7].source_ids["pad"], 9);
    // An accidentally queued Stop must not close the replacement's reader.
    assert_eq!(replacement.upload_effect(data).await.unwrap(), 9);
    assert_eq!(request(&new_requests), SourceRequest::Upload(data));
    assert!(matches!(new_requests.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn current_source_completion_detaches_and_preserves_target_definition() {
    let (source, requests) = mock_source(Ok(3));
    let mut sources = HashMap::from([("pad".to_owned(), source.clone())]);
    let mut effects = ForceFeedbackEffects::default();
    let data = rumble(100);
    assert_eq!(
        effects
            .upload_effect(7, data, &sources, &capabilities("pad"))
            .await,
        Some(7)
    );
    assert_eq!(request(&requests), SourceRequest::Upload(data));
    // The real completion notification arrives after its reader has exited.
    source.stop().await.unwrap();
    assert_eq!(request(&requests), SourceRequest::Stop);
    assert!(detach_source(&mut sources, &mut effects, "pad", Some(&source)).await);
    assert!(sources.is_empty());
    assert!(effects.effects[&7].source_ids.is_empty());
    assert_eq!(effects.effects[&7].data, data);
    assert!(!effects.available_ids.contains(&7));
}
