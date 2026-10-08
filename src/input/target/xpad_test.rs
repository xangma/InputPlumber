//! Real uinput regression checks. Run only in an isolated Linux VM/container.
use std::sync::mpsc;
use std::time::{Duration, Instant};

use evdev::{Device, FFEffectData, FFEffectKind, FFReplay, FFTrigger};
use nix::sys::stat::{makedev, mknod, Mode, SFlag};

use super::*;
use crate::input::composite_device::command::CompositeCommand;

fn rumble() -> FFEffectData {
    FFEffectData {
        direction: 0,
        trigger: FFTrigger {
            button: 0,
            interval: 0,
        },
        replay: FFReplay {
            length: 100,
            delay: 0,
        },
        kind: FFEffectKind::Rumble {
            strong_magnitude: 12000,
            weak_magnitude: 6000,
        },
    }
}

fn open_consumer(target: &mut XBoxController) -> Device {
    assert_eq!(
        std::env::var("INPUTPLUMBER_UINPUT_TESTS").as_deref(),
        Ok("1"),
        "uinput tests must be explicitly enabled in an isolated environment"
    );
    let node = target
        .device
        .lock()
        .unwrap()
        .enumerate_dev_nodes_blocking()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    if !node.exists() {
        // Containers expose sysfs but do not automatically create new device nodes.
        let name = node.file_name().unwrap().to_str().unwrap();
        let dev = std::fs::read_to_string(format!("/sys/class/input/{name}/dev")).unwrap();
        let (major, minor) = dev.trim().split_once(':').unwrap();
        std::fs::create_dir_all("/dev/input").unwrap();
        mknod(
            &node,
            SFlag::S_IFCHR,
            Mode::S_IRUSR | Mode::S_IWUSR,
            makedev(major.parse().unwrap(), minor.parse().unwrap()),
        )
        .unwrap();
    }
    let device = Device::open(node).unwrap();
    nix::fcntl::fcntl(device.as_fd(), FcntlArg::F_SETFL(OFlag::O_NONBLOCK)).unwrap();
    device
}

async fn poll_until_upload(
    target: &mut XBoxController,
    client: &Option<CompositeDeviceClient>,
    commands: &mut tokio::sync::mpsc::Receiver<CompositeCommand>,
) -> mpsc::Sender<Option<i16>> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let start = Instant::now();
        target.poll(client).unwrap();
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "rumble upload blocked target input polling for {:?}",
            start.elapsed()
        );
        if let Ok(CompositeCommand::ProcessOutputEvent(OutputEvent::Uinput(
            UinputOutputEvent::FFUpload(_, effect, reply),
        ))) = commands.try_recv()
        {
            assert_eq!(effect, rumble());
            return reply;
        }
        assert!(
            Instant::now() < deadline,
            "kernel upload never reached composite"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

fn release_stick(target: &mut XBoxController, observer: &mut Device) {
    let cap = Capability::Gamepad(Gamepad::Axis(GamepadAxis::LeftStick));
    target
        .write_event(NativeEvent::new(
            cap.clone(),
            InputValue::Vector2 {
                x: Some(0.75),
                y: Some(0.0),
            },
        ))
        .unwrap();
    // Read the input stream as Steam does. EVIOCGABS serializes behind an
    // in-flight upload in evdev_ioctl and would stall the observer itself.
    let events: Vec<_> = observer.fetch_events().unwrap().collect();
    assert!(events.iter().any(|event| matches!(event.destructure(),
        EventSummary::AbsoluteAxis(_, AbsoluteAxisCode::ABS_X, value) if value > 20000)));
    target
        .write_event(NativeEvent::new(
            cap,
            InputValue::Vector2 {
                x: Some(0.0),
                y: Some(0.0),
            },
        ))
        .unwrap();
    let events: Vec<_> = observer.fetch_events().unwrap().collect();
    assert!(
        events.iter().any(|event| matches!(event.destructure(),
        EventSummary::AbsoluteAxis(_, AbsoluteAxisCode::ABS_X, value) if value.abs() <= 1)),
        "released stick remained held while FF reply was pending"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Linux /dev/uinput"]
async fn ff_upload_pending_reply_does_not_hold_stick() {
    let mut target = XBoxController::new("xbox-elite").unwrap();
    let mut consumer = open_consumer(&mut target);
    let mut observer = open_consumer(&mut target);
    let (tx, mut commands) = tokio::sync::mpsc::channel(8);
    let client = Some(CompositeDeviceClient::new(tx));
    let (result_tx, result_rx) = mpsc::channel();
    let uploader = std::thread::spawn(move || {
        let result = consumer.upload_ff_effect(rumble());
        result_tx.send(result.is_ok()).unwrap();
        if let Ok(mut effect) = result {
            effect.play(1).unwrap();
            effect.stop().unwrap();
        }
    });
    let reply = poll_until_upload(&mut target, &client, &mut commands).await;
    assert!(
        result_rx.try_recv().is_err(),
        "upload completed before a composite response"
    );
    release_stick(&mut target, &mut observer);
    tokio::time::sleep(Duration::from_millis(200)).await;
    release_stick(&mut target, &mut observer);
    reply.send(Some(0)).unwrap();
    assert!(
        tokio::task::spawn_blocking(move || result_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap())
        .await
        .unwrap(),
        "valid rumble upload was rejected"
    );
    let mut forwarded = Vec::new();
    for _ in 0..50 {
        forwarded.extend(target.poll(&client).unwrap());
        if forwarded.len() >= 2 && uploader.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(uploader.is_finished(), "effect erase was not acknowledged");
    uploader.join().unwrap();
    assert!(
        forwarded
            .iter()
            .any(|event| matches!(event, OutputEvent::Evdev(event) if event.value() == 1)),
        "rumble start was not forwarded"
    );
    assert!(
        forwarded
            .iter()
            .any(|event| matches!(event, OutputEvent::Evdev(event) if event.value() == 0)),
        "rumble stop was not forwarded"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Linux /dev/uinput"]
async fn ff_upload_timeout_does_not_hold_stick() {
    let mut target = XBoxController::new("xbox-elite").unwrap();
    let mut consumer = open_consumer(&mut target);
    let mut observer = open_consumer(&mut target);
    let (tx, mut commands) = tokio::sync::mpsc::channel(8);
    let client = Some(CompositeDeviceClient::new(tx));
    let uploader = std::thread::spawn(move || consumer.upload_ff_effect(rumble()).is_ok());
    let _unanswered_reply = poll_until_upload(&mut target, &client, &mut commands).await;
    for _ in 0..25 {
        let start = Instant::now();
        target.poll(&client).unwrap();
        release_stick(&mut target, &mut observer);
        assert!(start.elapsed() < Duration::from_millis(100));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !uploader.join().unwrap(),
        "unanswered upload unexpectedly succeeded"
    );
}
