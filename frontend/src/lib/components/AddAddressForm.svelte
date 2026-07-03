<script lang="ts">
	import { api, ApiError } from '$lib/api/client';

	let { postcode }: { postcode: string } = $props();

	let open = $state(false);
	let submitting = $state(false);
	let done = $state(false);
	let errorMsg: string | null = $state(null);

	let house_number = $state('');
	let house_name = $state('');
	let flat = $state('');
	let street = $state('');
	let city = $state('');
	let county = $state('');

	const inputClass =
		'w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500';

	async function submit(e: Event) {
		e.preventDefault();
		errorMsg = null;
		if (!house_number.trim() && !house_name.trim() && !street.trim()) {
			errorMsg = 'Please enter at least a street or a house number/name.';
			return;
		}
		submitting = true;
		try {
			await api.submitAddress({
				postcode,
				house_number: house_number.trim() || undefined,
				house_name: house_name.trim() || undefined,
				flat: flat.trim() || undefined,
				street: street.trim() || undefined,
				city: city.trim() || undefined,
				county: county.trim() || undefined
			});
			done = true;
		} catch (err) {
			errorMsg =
				err instanceof ApiError ? err.detail : 'Something went wrong. Please try again.';
		} finally {
			submitting = false;
		}
	}
</script>

{#if done}
	<div class="rounded-lg border border-green-200 bg-green-50 p-4 text-sm text-green-700">
		✓ Thanks — your address has been submitted for review and will appear once approved.
	</div>
{:else if !open}
	<button
		class="text-sm font-medium text-blue-600 hover:text-blue-700"
		onclick={() => (open = true)}
	>
		+ Can't find your address? Add it
	</button>
{:else}
	<form onsubmit={submit} class="space-y-3 rounded-lg border border-gray-200 bg-gray-50 p-4">
		<p class="text-sm font-medium text-gray-700">
			Add a missing address for <strong>{postcode}</strong>
		</p>
		<div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
			<input class={inputClass} bind:value={house_number} placeholder="House number" />
			<input class={inputClass} bind:value={house_name} placeholder="House / building name" />
			<input class={inputClass} bind:value={flat} placeholder="Flat / unit (optional)" />
			<input class={inputClass} bind:value={street} placeholder="Street" />
			<input class={inputClass} bind:value={city} placeholder="Town / city" />
			<input class={inputClass} bind:value={county} placeholder="County (optional)" />
		</div>
		{#if errorMsg}
			<p class="text-sm text-red-600">{errorMsg}</p>
		{/if}
		<div class="flex gap-2">
			<button
				type="submit"
				disabled={submitting}
				class="rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-60"
			>
				{submitting ? 'Submitting…' : 'Submit for review'}
			</button>
			<button
				type="button"
				onclick={() => (open = false)}
				class="rounded-md border border-gray-300 px-4 py-2 text-sm font-medium text-gray-600 hover:bg-gray-100"
			>
				Cancel
			</button>
		</div>
		<p class="text-xs text-gray-400">
			Submissions are reviewed by an administrator before appearing in search.
		</p>
	</form>
{/if}
