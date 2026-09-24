# Smalltalk iOS

The four-tab Expo app uses the generated `st3.client.v0` TypeScript client. It connects only to a paired gateway over Tailscale HTTPS. The gateway URL and tab order stay in local app preferences; the paired bearer credential stays in iOS Keychain through `expo-secure-store`. No private endpoint or credential is built into the app.

## Connect

1. On a trusted st3 machine, begin device pairing for the intended person with `st3 devices --as person/... pair "iPhone"`.
2. On the iPhone, connect Tailscale, enter the **paired-only gateway** HTTPS URL, then enter the pairing ID and code. Never publish the privileged `st3.sock`.
3. Open Now, Chat, Control, and Fleet. The app shows offline/reconnect state; actions require a live connection. It does not queue offline mutations.

Now shows open actionable attention except transcript unread markers. Chat reads bounded normalized session history, sends messages through typed fenced actions, and can switch to a session terminal screen. Control shows bounded mission/work progress and launch creation, revision, preview, and approval. Fleet shows machines, connection identity, devices, and local tab ordering. Lists fetch at most 30 items per resource and refresh while foregrounded.

## Build locally

From this directory run `npm ci`, `npm run typecheck`, then `npx expo prebuild --platform ios --clean --no-install`. Install CocoaPods and open the generated `ios/SmalltalkStarter.xcworkspace` in Xcode. For daily development, build the Debug scheme for an iOS Simulator and run `npm run start` for Metro. Keep normal simulator code signing enabled: building with `CODE_SIGNING_ALLOWED=NO` leaves the app without a usable Keychain, so device pairing fails. A physical device is optional for this development proof.

For an offline device build, run `npm run export:ios` and build Release with local Apple Development signing and provisioning for that device. The Release bundle is embedded and runs without Metro. No Expo account, EAS service, App Store, or Shareup signing is part of this path.

The Debug app accepts a short-lived pairing deep link for headless simulator checks: `com.compoundingtech.smalltalk.starter://pair?gateway=...&id=...&code=...`. The handler is disabled in Release. Treat the link as a temporary credential and do not commit or log its populated form.

Generated `ios/`, build output, signing material, local configuration, and proof screenshots are ignored by Git. Do not commit Apple team/device IDs, credentials, machine paths, or private network addresses. Expo SDK 57 needs the `expo-build-properties` scene-lifecycle opt-in for Xcode 27/iOS 27.
