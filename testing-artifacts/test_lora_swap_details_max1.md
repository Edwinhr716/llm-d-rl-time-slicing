# Detailed Explanation of `test_lora_swap_max1.py`

This document explains the purpose and inner workings of the [test_lora_swap_max1.py](file:///usr/local/google/home/edwinhernandez/go/time-slicing-pr-repo/llm-d-rl-time-slicing/testing-artifacts/test_lora_swap_max1.py) script.

## High-Level Summary

The `test_lora_swap_max1.py` script is a test case designed to verify the **Checkpoint/Restore (CR)** mechanism for **LoRA** weights when vLLM is configured with `max_loras=1`. 

When `max_loras=1`, vLLM only allocates a single memory slot in GPU for LoRA weights. If we want to switch adapters, vLLM would normally evict the current adapter and load the new one from CPU/disk. This test verifies that we can use the Snapshot Agent to save and restore the raw GPU memory of this single slot to swap adapters, bypassing vLLM's slow load path.

To verify the swap without vLLM reloading the weights itself, the script uses a **"hijack" verification method**.

---

## Detailed Step-by-Step Walkthrough

### 1. Initialization and Setup
*   **Starts vLLM**: Initializes vLLM with `facebook/opt-125m` and `max_loras=1`. This pre-allocates exactly **one slot (Slot 0)** in GPU memory for LoRAs.
*   **Generates Dummy LoRAs**: Creates "LoRA A" (1x scale weights) and "LoRA B" (10x scale weights) in `/tmp/lora_A` and `/tmp/lora_B`.
*   **Connects to Snapshot Agent**: Connects to the host-side Snapshot Agent.

### 2. Loading and Snapshotting LoRA A
*   **Loads LoRA A**: Requests a generation using `lora_request_A`. vLLM loads LoRA A into **Slot 0**.
*   **Captures Initial Output A**: Runs a prompt and saves the output (`output_A_initial`).
*   **Finds Memory Addresses**: Queries the worker to find the GPU memory addresses for Slot 0 (`targets_A`).
*   **Snapshots LoRA A**: Calls the Snapshot Agent to save `targets_A` under Group `lora-group-A`.

### 3. Loading and Snapshotting LoRA B
*   **Loads LoRA B**: Requests a generation using `lora_request_B`.
    *   Since `max_loras=1`, vLLM **evicts** LoRA A and loads LoRA B into **Slot 0**.
*   **Captures Initial Output B**: Runs the same prompt and saves the output (`output_B_initial`).
*   **Finds Memory Addresses**: Finds the GPU memory addresses for Slot 0 (`targets_B`). These addresses must be identical to `targets_A` because they both use Slot 0.
*   **Snapshots LoRA B**: Calls the Snapshot Agent to save `targets_B` under Group `lora-group-B`.

### 4. Verification via Hijacking (The Core of the Test)
If we were to request LoRA A now, vLLM would see that B is in Slot 0 and would trigger its own reload mechanism, overwriting our restore. To prove the Snapshot Agent actually restores the weights, we hijack the active slot:

*   **Restore A**: We call the Snapshot Agent to restore the saved memory of LoRA A (from Group `lora-group-A`) into Slot 0.
    *   *GPU State*: Slot 0 now contains LoRA A's weights.
    *   *vLLM State*: vLLM still thinks LoRA B is active in Slot 0.
*   **Hijack Test A**: We run a query requesting **LoRA B** (`lora_request_B`).
    *   Since vLLM thinks B is already active, it does **not** trigger a reload. It directly uses the weights in Slot 0.
    *   **Expectation**: The output must match `output_A_initial` (LoRA A's output), proving that we successfully restored LoRA A's weights under the hood.
*   **Restore B**: We call the Snapshot Agent to restore the saved memory of LoRA B (from Group `lora-group-B`) into Slot 0.
*   **Verify B**: We run a query requesting **LoRA B** (`lora_request_B`).
    *   **Expectation**: The output must match `output_B_initial` (LoRA B's output).

---

## Why the Hijack Method is Necessary for `max_loras=1`

In a standard setup (`max_loras >= 2`), we have multiple slots, and we can keep both A and B "loaded" in vLLM's metadata. 

In a single-slot setup (`max_loras=1`), vLLM's metadata can only ever track one active LoRA. If we restore weights for "A" but vLLM's metadata says "B" is active, requesting "A" triggers a reload. The hijack method (restoring A but requesting B) is the only way to verify that the raw memory restore was successful without vLLM immediately overwriting it with a standard load.
