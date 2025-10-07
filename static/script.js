document.addEventListener('DOMContentLoaded', () => {
	// Показываем сообщение и overlay, если есть msg
	if (typeof hasMsg !== 'undefined' && hasMsg) {
		const overlay = document.getElementById('overlay');
		const saveMsg = document.getElementById('save-msg');

		if (overlay && saveMsg) {
			overlay.style.display = 'block';
			saveMsg.style.display = 'block';

			setTimeout(() => {
				saveMsg.style.display = 'none';
				overlay.style.display = 'none';
			}, 1000); // 1 секунда
		}
	}

	// Убираем msg из URL и со страницы
	setTimeout(() => {
		const url = new URL(window.location.href);
		if (url.searchParams.has('msg')) {
			url.searchParams.delete('msg');
			history.replaceState(null, '', url.pathname + (url.searchParams.toString() ? '?' + url.searchParams.toString() : ''));
		}

		const msg = document.querySelector('.msg-green');
		if (msg) msg.remove();
	}, 1000); // 1 секунда

	// Функция сохраняет текущую позицию скролла перед отправкой формы
	function saveScrollPosition() {
		localStorage.setItem('scrollPos', window.scrollY);
	}

	// На все формы в таблице вешаем событие перед отправкой
	document.querySelectorAll('form').forEach(form => {
		form.addEventListener('submit', saveScrollPosition);
	});

	// После загрузки страницы восстанавливаем скролл
	window.addEventListener('load', () => {
		const scrollPos = localStorage.getItem('scrollPos');
		if (scrollPos) {
			window.scrollTo(0, parseInt(scrollPos));
			localStorage.removeItem('scrollPos'); // чтобы при следующей загрузке не скроллило
		}
	});

	// Добавляем функционал сворачивания длинных текстов
	document.querySelectorAll('td').forEach(td => {
		const text = td.textContent.trim();
		if (text.length > 15) { // если текст длинный
			td.classList.add('collapsed');

			const textSpan = document.createElement('span');
			textSpan.className = 'text-content';
			textSpan.textContent = text;

			td.textContent = '';
			td.appendChild(textSpan);

			const btn = document.createElement('span');
			btn.textContent = 'Показать';
			btn.className = 'expand-btn';
			btn.style.display = 'block';          // кнопка теперь на новой строке
			btn.style.marginTop = '5px';          // небольшой отступ сверху

			btn.addEventListener('click', () => {
				td.classList.toggle('collapsed');
				td.classList.toggle('expanded');
				btn.textContent = td.classList.contains('collapsed') ? 'Показать' : 'Скрыть';
			});

			td.appendChild(btn);
		}
	});
});
