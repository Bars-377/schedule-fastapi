document.addEventListener('DOMContentLoaded', () => {
	// Показываем сообщение и overlay, если есть msg
	const urlParams = new URLSearchParams(window.location.search);
	const msgStatus = parseInt(urlParams.get('status')) || 200;
	const hasMsg = urlParams.has('msg');
	if (hasMsg) {
		const overlay = document.getElementById('overlay');
		const saveMsg = document.getElementById('save-msg');

		console.log('Есть сообщение', msgStatus);

		if (overlay && saveMsg) {
			// Меняем цвет в зависимости от status_code
			if (typeof msgStatus !== 'undefined') {
				if (msgStatus >= 200 && msgStatus < 300) {
					console.log('Зелёный цвет должен примениться');
					saveMsg.style.backgroundColor = 'rgba(0, 128, 0, 0.9)'; // зелёный
				} else if (msgStatus >= 400 && msgStatus < 500) {
					console.log('Оранжевый цвет');
					saveMsg.style.backgroundColor = 'rgba(255, 165, 0, 0.9)'; // оранжевый
				} else if (msgStatus >= 500) {
					console.log('Красный цвет');
					saveMsg.style.backgroundColor = 'rgba(255, 0, 0, 0.9)'; // красный
				} else {
					console.log('Синий цвет');
					saveMsg.style.backgroundColor = 'rgba(0, 0, 128, 0.9)'; // синий для прочих
				}
			}

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
			history.replaceState(
				null,
				'',
				url.pathname + (url.searchParams.toString() ? '?' + url.searchParams.toString() : '')
			);
		}

		const saveMsg = document.getElementById('save-msg');
		if (saveMsg) saveMsg.remove();
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

	const colors = ["#4F83CC", "green"];
	let index = 0;

	document.querySelectorAll("table tbody tr").forEach(tr => {
		const color = colors[index % colors.length]; // выбираем цвет по очереди
		index++;
		tr.querySelectorAll("td").forEach(td => {
			td.style.borderBottom = "1px solid " + color; // только нижняя рамка
		});
	});

});
