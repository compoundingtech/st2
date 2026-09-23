# Smalltalk iOS starter

This is a four-tab Expo render shell with no ST3 connection, credentials, actions, or push
notifications. It is deliberately not a production app.

From this directory: `npm ci`, `npm run typecheck`, and `npm run export:ios`. The export proves the
iOS-targeted JavaScript bundle compiles; it does **not** prove native iOS compilation or on-device
rendering. `npm run start` starts Expo for a simulator or Expo Go after an authenticated device
workflow is available.

`eas.json` defines simulator and internal-preview build profiles, but an EAS build needs an Expo
login and project setup. A physical iPhone build also needs Apple signing and a device provisioning
path. Do not distribute this starter as a production release.

Expo SDK 57 needs the `expo-build-properties` scene-lifecycle opt-in when built with Xcode 27/iOS 27;
without it the native app builds and installs but crashes at launch. Keep this setting until an SDK
upgrade provides scene support by default.
