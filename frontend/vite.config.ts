import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	server: {
		// Enable polling so file-watch works inside Docker on Windows/macOS
		watch: {
			usePolling: true
		}
	}
});
