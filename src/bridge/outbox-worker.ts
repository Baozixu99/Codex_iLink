import { createHash } from "node:crypto";
import {
  SqliteState,
  type NotificationRoute,
  type OutboxItem,
} from "./sqlite-state.ts";
import type { ILinkSender } from "./bridge.ts";
import { ILinkError, type ILinkSession } from "../ilink/protocol.ts";
import {
  parseDesktopNotificationClientId,
  terminalNotificationExpiry,
} from "./desktop-notification-identity.ts";
import { dispatchOutboxItem } from "./outbox-delivery.ts";
import { WECHAT_FINAL_MAX_MESSAGES } from "./wechat-output.ts";

export type OutboxDrainResult = {
  confirmed: number;
  deferred: number;
  failed: number;
};

export type OutboxWorkerOptions = {
  canSendNotifications?: () => boolean;
  ilink: ILinkSender;
  maxAttempts?: number;
  now: () => number;
  outboundDirectory?: string;
  onConfirmed?: (
    item: Pick<
      OutboxItem,
      "body" | "clientId" | "contextToken" | "targetUserId"
    >,
    confirmedAtMs: number,
  ) => void;
  routeOnConfirmed?: (
    item: Pick<OutboxItem, "clientId">,
    confirmedAtMs: number,
  ) => NotificationRoute | null;
  session: ILinkSession;
  sleep?: (milliseconds: number, signal?: AbortSignal) => Promise<void>;
  state: SqliteState;
};

const NOTIFICATION_SUMMARY_PREFIX = "codex-ilink:notification-summary:";

/** Replays durable pending sends with one stable iLink client_id. */
export class OutboxWorker {
  readonly #canSendNotifications: () => boolean;
  readonly #failedForRun = new Set<string>();
  readonly #ilink: ILinkSender;
  readonly #maxAttempts: number;
  readonly #now: () => number;
  readonly #outboundDirectory: string | undefined;
  readonly #onConfirmed: OutboxWorkerOptions["onConfirmed"];
  readonly #routeOnConfirmed: OutboxWorkerOptions["routeOnConfirmed"];
  readonly #session: ILinkSession;
  readonly #sleep: NonNullable<OutboxWorkerOptions["sleep"]>;
  readonly #state: SqliteState;
  #draining: Promise<OutboxDrainResult> | undefined;

  constructor(options: OutboxWorkerOptions) {
    this.#canSendNotifications = options.canSendNotifications ?? (() => true);
    this.#ilink = options.ilink;
    this.#maxAttempts = options.maxAttempts ?? 3;
    this.#now = options.now;
    this.#outboundDirectory = options.outboundDirectory;
    this.#onConfirmed = options.onConfirmed;
    this.#routeOnConfirmed = options.routeOnConfirmed;
    this.#session = options.session;
    this.#sleep = options.sleep ?? abortableSleep;
    this.#state = options.state;
    if (!Number.isSafeInteger(this.#maxAttempts) || this.#maxAttempts < 1) {
      throw new Error("E_OUTBOX_ATTEMPTS");
    }
  }

  drain(signal?: AbortSignal): Promise<OutboxDrainResult> {
    if (this.#draining) return this.#draining;
    const draining = this.#drain(signal).finally(() => {
      if (this.#draining === draining) this.#draining = undefined;
    });
    this.#draining = draining;
    return draining;
  }

  /** Allows one new bounded retry cycle after genuine controller activity. */
  resetDeferred(): number {
    const count = this.#failedForRun.size;
    this.#failedForRun.clear();
    return count;
  }

  async #drain(signal?: AbortSignal): Promise<OutboxDrainResult> {
    if (signal?.aborted) throw signal.reason;
    const result: OutboxDrainResult = { confirmed: 0, deferred: 0, failed: 0 };
    const blockedFinalReplies = new Set<string>();
    for (const item of this.#preparePendingItems()) {
      if (signal?.aborted) throw signal.reason;
      const finalReply = finalReplyGroup(item.clientId);
      const expiresAtMs = notificationExpiry(item);
      const latestContext = this.#state.getILinkState(this.#session.botId)?.contextToken;
      const contextToken = item.clientId.startsWith(NOTIFICATION_SUMMARY_PREFIX)
        ? latestContext || item.contextToken
        : item.contextToken || latestContext;
      if (
        item.body === null ||
        !contextToken ||
        this.#failedForRun.has(item.clientId) ||
        (finalReply !== null && blockedFinalReplies.has(finalReply))
      ) {
        if (finalReply !== null) blockedFinalReplies.add(finalReply);
        result.deferred += 1;
        continue;
      }

      let confirmed = false;
      let deferred = false;
      for (let attempt = 1; attempt <= this.#maxAttempts; attempt += 1) {
        if (expiresAtMs !== undefined) {
          const nowMs = this.#now();
          if (expiresAtMs <= nowMs) {
            this.#discardNotifications([item.clientId], nowMs);
            deferred = true;
            break;
          }
          if (!this.#canSendNotifications()) {
            result.deferred += 1;
            deferred = true;
            break;
          }
        }
        try {
          const current = this.#state.getOutbox(item.clientId);
          if (!current || current.status !== "pending" || current.body === null) {
            break;
          }
          const dispatched = await dispatchOutboxItem({
            contextToken,
            ilink: this.#ilink,
            item: current,
            ...(this.#outboundDirectory
              ? { outboundDirectory: this.#outboundDirectory }
              : {}),
            session: this.#session,
            ...(signal ? { signal } : {}),
            state: this.#state,
          });
          const confirmedAtMs = this.#now();
          const route =
            this.#routeOnConfirmed?.(dispatched, confirmedAtMs) ?? undefined;
          this.#state.confirmOutbox(item.clientId, confirmedAtMs, route);
          try {
            this.#onConfirmed?.(dispatched, confirmedAtMs);
          } catch {
            // Delivery is already accepted and must never be retried merely
            // because an optional local post-confirmation hook failed.
          }
          result.confirmed += 1;
          confirmed = true;
          break;
        } catch (error) {
          if (signal?.aborted) throw error;
          if (error instanceof ILinkError && error.kind === "auth-expired") {
            throw error;
          }
          if (attempt < this.#maxAttempts) {
            await this.#sleep(250 * 2 ** (attempt - 1), signal);
          }
        }
      }
      if (!confirmed && !deferred) {
        this.#failedForRun.add(item.clientId);
        if (finalReply !== null) blockedFinalReplies.add(finalReply);
        result.failed += 1;
      }
    }
    return result;
  }

  #preparePendingItems(): OutboxItem[] {
    const nowMs = this.#now();
    const replies: OutboxItem[] = [];
    const summaries: OutboxItem[] = [];
    const notifications: OutboxItem[] = [];
    const notificationIds = new Set<string>();
    const expired: string[] = [];
    let summaryExpiry = Infinity;
    for (const item of this.#state.listPendingOutbox()) {
      const expiresAtMs = notificationExpiry(item);
      if (expiresAtMs === undefined) {
        replies.push(item);
        continue;
      }
      if (expiresAtMs <= nowMs) {
        expired.push(item.clientId);
        continue;
      }
      if (item.clientId.startsWith(NOTIFICATION_SUMMARY_PREFIX)) {
        summaries.push(item);
        continue;
      }
      const identity = parseDesktopNotificationClientId(item.clientId);
      if (!identity) continue;
      notifications.push(item);
      notificationIds.add(identity.baseClientId);
      summaryExpiry = Math.min(summaryExpiry, expiresAtMs);
    }
    if (expired.length > 0) this.#discardNotifications(expired, nowMs);
    if (!this.#canSendNotifications()) return replies;
    // A persisted summary keeps its identity and is retried before a new batch.
    if (summaries.length > 0) return [...replies, ...summaries];
    if (notificationIds.size < 2) return [...replies, ...notifications];

    const clientIds = notifications.map(({ clientId }) => clientId);
    const hash = createHash("sha256").update(JSON.stringify(clientIds)).digest("hex");
    const summary = this.#state.discardPendingNotifications(clientIds, nowMs, {
      body: `有 ${notificationIds.size} 个 Codex 任务的通知积压。请在 Codex Desktop 查看详情，或发 p 选择项目后进入对应会话。`,
      clientId: NOTIFICATION_SUMMARY_PREFIX + hash,
      contextToken: notifications.at(-1)?.contextToken ?? "",
      createdAtMs: nowMs,
      expiresAtMs: summaryExpiry,
      targetUserId: this.#session.controllerUserId,
    });
    for (const clientId of clientIds) this.#failedForRun.delete(clientId);
    return summary ? [...replies, summary] : replies;
  }

  #discardNotifications(clientIds: readonly string[], nowMs: number): void {
    this.#state.discardPendingNotifications(clientIds, nowMs);
    for (const clientId of clientIds) this.#failedForRun.delete(clientId);
  }
}

function notificationExpiry(item: OutboxItem): number | undefined {
  if (item.expiresAtMs !== undefined) return item.expiresAtMs;
  const identity = parseDesktopNotificationClientId(item.clientId);
  return identity
    ? terminalNotificationExpiry(identity.origin, item.createdAtMs)
    : undefined;
}

function finalReplyGroup(clientId: string): string | null {
  const bridge = /^(codex-ilink:[^:]+:final)(?::part:(\d+))?$/u.exec(clientId);
  const baseClientId = bridge?.[1];
  const part = bridge?.[2];
  const partNumber = part === undefined ? null : Number(part);
  if (
    baseClientId &&
    (partNumber === null ||
      (Number.isSafeInteger(partNumber) &&
        partNumber >= 1 &&
        partNumber <= WECHAT_FINAL_MAX_MESSAGES))
  ) {
    return baseClientId;
  }
  return parseDesktopNotificationClientId(clientId)?.baseClientId ?? null;
}

function abortableSleep(
  milliseconds: number,
  signal?: AbortSignal,
): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const finish = (): void => {
      signal?.removeEventListener("abort", abort);
      resolve();
    };
    const timer = setTimeout(finish, milliseconds);
    const abort = (): void => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    signal?.addEventListener("abort", abort, { once: true });
    // A later caller can join drain(), so the retry must keep the process alive
    // until the shared promise settles on every supported Node version.
  });
}
