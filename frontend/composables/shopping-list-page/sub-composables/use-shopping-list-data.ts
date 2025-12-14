import { useOnline, useIdle } from "@vueuse/core";
import type { ShoppingListOut } from "~/lib/api/types/household";
import { useShoppingListItemActions } from "~/composables/use-shopping-list-item-actions";
import { useShoppingListWebSocket } from "~/composables/use-shopping-list-websocket";

/**
 * Composable for managing shopping list data fetching and polling
 */
export function useShoppingListData(listId: string, shoppingList: Ref<ShoppingListOut | null>, loadingCounter: Ref<number>) {
  const isOffline = computed(() => useOnline().value === false);
  const { idle } = useIdle(5 * 60 * 1000); // 5 minutes
  const shoppingListItemActions = useShoppingListItemActions(listId);

  async function fetchShoppingList() {
    const data = await shoppingListItemActions.getList();
    return data;
  }

  async function refresh(updateListItemOrder: () => void) {
    loadingCounter.value += 1;
    try {
      await shoppingListItemActions.process();
    }
    catch (error) {
      console.error(error);
    }

    let newListValue: typeof shoppingList.value = null;
    try {
      newListValue = await fetchShoppingList();
    }
    catch (error) {
      console.error(error);
    }

    loadingCounter.value -= 1;

    // only update the list with the new value if we're not loading, to prevent UI jitter
    if (loadingCounter.value) {
      return;
    }

    // Prevent overwriting local changes with stale backend data when offline
    if (isOffline.value) {
      // Do not update shoppingList.value from backend when offline
      updateListItemOrder();
      return;
    }

    // if we're not connected to the network, this will be null, so we don't want to clear the list
    if (newListValue) {
      shoppingList.value = newListValue;
    }

    updateListItemOrder();
  }

  // constantly polls for changes
  async function pollForChanges(updateListItemOrder: () => void) {
    // pause polling if the user isn't active or we're busy
    if (idle.value || loadingCounter.value) {
      return;
    }

    // Skip polling if WebSocket is connected (we'll get real-time updates instead)
    if (websocket?.isConnected?.value) {
      return;
    }

    try {
      await refresh(updateListItemOrder);

      if (shoppingList.value) {
        attempts = 0;
        return;
      }

      // if the refresh was unsuccessful, the shopping list will be null, so we increment the attempt counter
      attempts++;
    }
    catch {
      attempts++;
    }

    // if we hit too many errors, stop polling
    if (attempts >= maxAttempts) {
      clearInterval(pollTimer);
    }
  }

  // start polling
  loadingCounter.value -= 1;

  // max poll time = pollFrequency * maxAttempts = 24 hours
  // we use a long max poll time since polling stops when the user is idle anyway
  const pollFrequency = 5000;
  const maxAttempts = 17280;
  let attempts = 0;
  let pollTimer: ReturnType<typeof setInterval>;
  let updateListItemOrderFn: (() => void) | null = null;

  // Initialize WebSocket for real-time updates
  const user = useAuthBackend();
  const householdId = computed(() => user.user.value?.householdId || "");

  // WebSocket instance holder
  let websocket: ReturnType<typeof useShoppingListWebSocket> | null = null;

  function initializeWebSocket() {
    if (!householdId.value || websocket) {
      return;
    }

    websocket = useShoppingListWebSocket(householdId.value, {
      onShoppingListCreated: (data) => {
        // A new list was created, but we're on a specific list page, so we don't need to do anything
        console.log("Shopping list created:", data);
      },
      onShoppingListUpdated: (data) => {
        // Shopping list or items were updated, refresh the current list
        if (data && updateListItemOrderFn) {
          console.log("Shopping list updated via WebSocket, refreshing...");
          refresh(updateListItemOrderFn);
        }
      },
      onShoppingListDeleted: (data) => {
        // List was deleted, but we're on the list page, so the user will see an error when trying to interact
        console.log("Shopping list deleted:", data);
      },
    });
  }

  function startPolling(updateListItemOrder: () => void) {
    updateListItemOrderFn = updateListItemOrder;
    pollForChanges(updateListItemOrder); // populate initial list

    pollTimer = setInterval(() => {
      pollForChanges(updateListItemOrder);
    }, pollFrequency);

    // Connect to WebSocket for real-time updates once householdId is available
    initializeWebSocket();
    if (websocket && householdId.value) {
      websocket.connect();
    }
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
    }

    // Disconnect WebSocket
    if (websocket) {
      websocket.disconnect();
      websocket = null;
    }
  }

  return {
    isOffline,
    fetchShoppingList,
    refresh,
    startPolling,
    stopPolling,
    shoppingListItemActions,
    websocket: computed(() => websocket),
  };
}
