<script lang="ts">
	import { api, ApiError } from '$lib/api/client';
	import type { PostcodeAutocompleteItem, PostcodeLookupResponse } from '$lib/api/types';
	import Spinner from './Spinner.svelte';

	let {
		onresult,
		onerror,
		onloadingchange
	}: {
		onresult: (result: PostcodeLookupResponse) => void;
		onerror: (error: string) => void;
		onloadingchange: (loading: boolean) => void;
	} = $props();

	let query = $state('');
	let suggestions: PostcodeAutocompleteItem[] = $state([]);
	let showDropdown = $state(false);
	let activeIndex = $state(-1);
	let searching = $state(false);
	let inputEl: HTMLInputElement | undefined = $state();

	// Debounced autocomplete
	let debounceTimer: ReturnType<typeof setTimeout> | undefined;

	$effect(() => {
		const q = query.trim();
		clearTimeout(debounceTimer);

		if (q.length < 2) {
			suggestions = [];
			showDropdown = false;
			return;
		}

		debounceTimer = setTimeout(async () => {
			try {
				const data = await api.autocomplete(q);
				suggestions = data.results;
				showDropdown = suggestions.length > 0;
				activeIndex = -1;
			} catch {
				suggestions = [];
				showDropdown = false;
			}
		}, 300);

		return () => clearTimeout(debounceTimer);
	});

	async function selectPostcode(postcodeNoSpace: string) {
		showDropdown = false;
		suggestions = [];
		searching = true;
		onloadingchange(true);

		try {
			const result = await api.lookupPostcode(postcodeNoSpace);
			query = result.postcode.postcode;
			onresult(result);
		} catch (err) {
			if (err instanceof ApiError) {
				onerror(err.detail);
			} else {
				onerror('Failed to look up postcode. Please try again.');
			}
		} finally {
			searching = false;
			onloadingchange(false);
		}
	}

	function handleKeydown(e: KeyboardEvent) {
		if (!showDropdown) {
			if (e.key === 'Enter') {
				e.preventDefault();
				const q = query.trim().replace(/\s/g, '');
				if (q) selectPostcode(q);
			}
			return;
		}

		switch (e.key) {
			case 'ArrowDown':
				e.preventDefault();
				activeIndex = (activeIndex + 1) % suggestions.length;
				break;
			case 'ArrowUp':
				e.preventDefault();
				activeIndex = activeIndex <= 0 ? suggestions.length - 1 : activeIndex - 1;
				break;
			case 'Enter':
				e.preventDefault();
				if (activeIndex >= 0 && activeIndex < suggestions.length) {
					selectPostcode(suggestions[activeIndex].postcode_no_space);
				} else {
					const q = query.trim().replace(/\s/g, '');
					if (q) selectPostcode(q);
				}
				break;
			case 'Escape':
				showDropdown = false;
				activeIndex = -1;
				break;
		}
	}

	function handleClickOutside(e: MouseEvent) {
		const target = e.target as HTMLElement;
		if (!target.closest('.postcode-search')) {
			showDropdown = false;
			activeIndex = -1;
		}
	}
</script>

<svelte:window onclick={handleClickOutside} />

<div class="postcode-search relative">
	<div class="relative">
		<input
			bind:this={inputEl}
			bind:value={query}
			onkeydown={handleKeydown}
			type="text"
			placeholder="Enter a postcode (e.g. SW1A 1AA)"
			class="w-full rounded-lg border border-gray-300 px-4 py-3 text-lg shadow-sm transition-colors focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
			autocomplete="off"
			aria-label="Search postcodes"
			aria-expanded={showDropdown}
			aria-controls="postcode-suggestions"
			role="combobox"
			aria-autocomplete="list"
			aria-activedescendant={activeIndex >= 0 ? `suggestion-${activeIndex}` : undefined}
		/>
		{#if searching}
			<div class="absolute right-3 top-1/2 -translate-y-1/2 text-blue-500">
				<Spinner />
			</div>
		{/if}
	</div>

	{#if showDropdown}
		<ul
			id="postcode-suggestions"
			class="absolute z-10 mt-1 w-full overflow-hidden rounded-lg border border-gray-200 bg-white shadow-lg"
			role="listbox"
		>
			{#each suggestions as suggestion, i (suggestion.postcode_no_space)}
				<li
					id="suggestion-{i}"
					role="option"
					aria-selected={i === activeIndex}
					class="cursor-pointer px-4 py-2 text-sm transition-colors {i === activeIndex
						? 'bg-blue-50 text-blue-900'
						: 'text-gray-700 hover:bg-gray-50'}"
					onclick={() => selectPostcode(suggestion.postcode_no_space)}
					onkeydown={(e) => {
						if (e.key === 'Enter') selectPostcode(suggestion.postcode_no_space);
					}}
					onmouseenter={() => (activeIndex = i)}
				>
					{suggestion.postcode}
				</li>
			{/each}
		</ul>
	{/if}
</div>
