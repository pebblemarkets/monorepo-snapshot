export type RetryOptions = {
  attempts?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  backoffMultiplier?: number;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

export async function withRetry<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {},
): Promise<T> {
  const attempts = Math.max(1, options.attempts ?? 2);
  const initialDelayMs = Math.max(0, options.initialDelayMs ?? 250);
  const maxDelayMs = Math.max(initialDelayMs, options.maxDelayMs ?? 1_500);
  const backoffMultiplier = Math.max(1, options.backoffMultiplier ?? 2);

  let delayMs = initialDelayMs;
  let lastError: unknown;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await operation();
    } catch (err: unknown) {
      lastError = err;
      if (attempt >= attempts) {
        break;
      }

      await sleep(delayMs);
      delayMs = Math.min(maxDelayMs, delayMs * backoffMultiplier);
    }
  }

  throw lastError;
}
