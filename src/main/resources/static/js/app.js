document.addEventListener('DOMContentLoaded', () => {
	const bucketForm = document.querySelector('#bucketForm');
	const bucketSelect = document.querySelector('#bucketSelect');
	const sourceSelect = document.querySelector('#sourceSelect');
	const selectionForm = document.querySelector('#selectionForm');
	const selectAllCheckbox = document.querySelector('#selectAll');
	const downloadButton = document.querySelector('#downloadSelectedButton');
	const previewToggle = document.querySelector('#previewToggle');

	const prefixInput = bucketForm ? bucketForm.querySelector('input[name="prefix"]') : null;
	const tokenStackInput = bucketForm ? bucketForm.querySelector('input[name="tokenStack"]') : null;

	const resetNavigationState = () => {
		// clear path state so source/bucket switches always land at the root listing
		if (prefixInput) {
			prefixInput.value = '';
		}
		if (tokenStackInput) {
			tokenStackInput.value = '';
		}
	};

	const updateDownloadButtonState = () => {
		if (!downloadButton || !selectionForm) {
			return;
		}

		const selected = selectionForm.querySelectorAll('input[name="keys"]:checked');
		downloadButton.disabled = selected.length === 0;
	};

	const applyPreviewState = enabled => {
		if (!previewToggle) {
			return;
		}

		previewToggle.setAttribute('aria-pressed', String(enabled));
		const label = previewToggle.querySelector('span');
		if (label) {
			label.textContent = enabled ? 'Hide previews' : 'Show previews';
		}

		document.querySelectorAll('.preview-thumb').forEach(container => {
			if (!(container instanceof HTMLElement)) {
				return;
			}

			if (enabled) {
				container.classList.remove('hidden');
				const image = container.querySelector('img');
				if (image && !image.dataset.loaded) {
					image.src = container.dataset.previewUrl;
					image.dataset.loaded = 'true';
				}
			} else {
				container.classList.add('hidden');
			}
		});

		try {
			localStorage.setItem('s3-downloader-previews', String(enabled));
		} catch (error) {
			console.warn('Failed to persist preview toggle state', error);
		}
	};

	if (bucketForm && bucketSelect) {
		bucketSelect.addEventListener('change', () => {
			resetNavigationState();
			bucketForm.submit();
		});
	}

	if (bucketForm && sourceSelect) {
		sourceSelect.addEventListener('change', () => {
			resetNavigationState();
			if (bucketSelect) {
				bucketSelect.disabled = true;
			}
			bucketForm.submit();
			setTimeout(() => {
				if (bucketSelect) {
					bucketSelect.disabled = false;
				}
			}, 0);
		});
	}

	if (selectionForm && selectAllCheckbox) {
		selectAllCheckbox.addEventListener('change', event => {
			const checked = event.target.checked;
			selectionForm.querySelectorAll('input[name="keys"]').forEach(checkbox => {
				checkbox.checked = checked;
			});
			updateDownloadButtonState();
		});
	}

	if (selectionForm) {
		selectionForm.addEventListener('change', event => {
			if (event.target && event.target.name === 'keys') {
				const all = selectionForm.querySelectorAll('input[name="keys"]').length;
				const checked = selectionForm.querySelectorAll('input[name="keys"]:checked').length;
				if (selectAllCheckbox) {
					selectAllCheckbox.indeterminate = checked > 0 && checked < all;
					selectAllCheckbox.checked = checked > 0 && checked === all;
				}
				updateDownloadButtonState();
			}
		});
	}

	if (previewToggle) {
		const stored = (() => {
			try {
				return localStorage.getItem('s3-downloader-previews');
			} catch (error) {
				console.warn('Failed to read preview toggle state', error);
				return null;
			}
		})();

		const initialState = stored === 'true';
		applyPreviewState(initialState);

		previewToggle.addEventListener('click', () => {
			const nextState = previewToggle.getAttribute('aria-pressed') !== 'true';
			applyPreviewState(nextState);
		});
	}

	updateDownloadButtonState();
});
