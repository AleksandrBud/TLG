async def process_films(dialog: object):
    # Обработка чатов с видео
    msg_list = []
    lst_date = proc_chat.get_last_date(dialog.id, session)
    # list_chat_errors = get_chat_errors(dialog.id)
    async for msg in client.iter_messages(dialog):
        if lst_date < msg.date.replace(tzinfo=None) \
                and msg.video:
            msg_list.append(msg)