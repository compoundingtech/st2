# Smalltalk iOS starter

This is a four-tab Expo render shell with no ST3 connection, credentials, actions, or push
notifications. It is deliberately not a production app.

From this directory: `npm ci`, `npm run typecheck`, and `npm run export:ios`. The export proves the
iOS-targeted JavaScript bundle compiles; it does **not** prove native iOS compilation or on-device
rendering. `npm run start` starts Metro. A Debug build installed from Xcode needs Metro running to
load its JavaScript; otherwise the app shows “No script URL provided.” Start Metro from this
directory and reopen the app on the device. The Mac and iPhone must be able to reach each other,
either on the same LAN or through a private network such as Tailscale. Local Xcode builds do not
require an Expo account.

`eas.json` defines simulator and internal-preview build profiles, but an EAS build needs an Expo
login and project setup. A physical iPhone build also needs Apple signing and a device provisioning
path. Do not distribute this starter as a production release.

The generated `ios/` project, local build output, signing material, environment files, and proof
screenshots are ignored by Git. Keep personal team IDs, device identifiers, and private network
addresses out of tracked app configuration and documentation.

Expo SDK 57 needs the `expo-build-properties` scene-lifecycle opt-in when built with Xcode 27/iOS 27;
without it the native app builds and installs but crashes at launch. Keep this setting until an SDK
upgrade provides scene support by default.
