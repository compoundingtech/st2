import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  pi.registerProvider("fakelab", {
    name: "Fake Lab",
    baseUrl: "http://127.0.0.1:8917/v1",
    apiKey: "not-a-secret",
    api: "openai-completions",
    models: [
      {
        id: "fake-1",
        name: "Fake 1",
        reasoning: false,
        input: ["text"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 128000,
        maxTokens: 4096,
      },
    ],
  });
}
