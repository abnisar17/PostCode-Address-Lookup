<script lang="ts">
	let {
		currentPage,
		totalPages,
		onpagechange
	}: {
		currentPage: number;
		totalPages: number;
		onpagechange: (page: number) => void;
	} = $props();

	let pages = $derived.by(() => {
		if (totalPages <= 7) {
			return Array.from({ length: totalPages }, (_, i) => i + 1);
		}

		const items: (number | null)[] = [1];

		if (currentPage > 3) {
			items.push(null); // ellipsis
		}

		const start = Math.max(2, currentPage - 1);
		const end = Math.min(totalPages - 1, currentPage + 1);
		for (let i = start; i <= end; i++) {
			items.push(i);
		}

		if (currentPage < totalPages - 2) {
			items.push(null); // ellipsis
		}

		items.push(totalPages);
		return items;
	});
</script>

<nav aria-label="Pagination" class="flex items-center justify-center gap-1">
	<button
		class="rounded-md px-3 py-1.5 text-sm text-gray-600 transition-colors hover:bg-gray-100 disabled:cursor-not-allowed disabled:opacity-40"
		disabled={currentPage <= 1}
		onclick={() => onpagechange(currentPage - 1)}
	>
		Previous
	</button>

	{#each pages as page, i (page != null ? page : `ellipsis-${i}`)}
		{#if page === null}
			<span class="px-2 text-gray-400">...</span>
		{:else}
			<button
				class="min-w-[2rem] rounded-md px-2 py-1.5 text-sm transition-colors {page === currentPage
					? 'bg-blue-500 font-medium text-white'
					: 'text-gray-600 hover:bg-gray-100'}"
				aria-current={page === currentPage ? 'page' : undefined}
				onclick={() => onpagechange(page)}
			>
				{page}
			</button>
		{/if}
	{/each}

	<button
		class="rounded-md px-3 py-1.5 text-sm text-gray-600 transition-colors hover:bg-gray-100 disabled:cursor-not-allowed disabled:opacity-40"
		disabled={currentPage >= totalPages}
		onclick={() => onpagechange(currentPage + 1)}
	>
		Next
	</button>
</nav>
