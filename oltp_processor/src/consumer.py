import asyncio
import logging

from aiokafka import AIOKafkaConsumer

import db

logger = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(
        self,
        host: str,
        port: int,
        topic: str,
        group_id: str = "python-oltp-consumer",
        batch_size: int = 1000,
        batch_timeout: float = 5.0,
        read_timeout: int = 1000,
        database_url: str | None = None,
    ):
        self.host = host
        self.port = port
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.read_timeout = read_timeout

        self.consumer = None
        self.db_processor = None
        self.db_url = database_url

    async def start(self):
        """Инициализация консьюмера и БД"""
        logger.info(
            f"Starting Kafka consumer: {self.host}:{self.port}, "
            f"topic={self.topic}, group={self.group_id}"
        )

        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=f"{self.host}:{self.port}",
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Ручной коммит после успешной вставки
            max_poll_records=self.batch_size,  # Получаем сразу батч
        )
        self.db_processor = db.DBProcessor(self.db_url)

        await self.consumer.start()
        logger.info("Kafka consumer started successfully")

    async def stop(self):
        """Остановка консьюмера и закрытие БД"""
        logger.info("Stopping Kafka consumer...")

        if self.consumer:
            await self.consumer.stop()

        await self.db_processor.close()
        logger.info("Kafka consumer stopped")

    def should_insert(self, batch, time_elapsed):
        return len(batch) >= self.batch_size or (
            batch and time_elapsed >= self.batch_timeout
        )

    async def get_batch(self):
        data = await self.consumer.getmany(
            timeout_ms=self.read_timeout, max_records=self.batch_size
        )
        batch = []
        for messages in data.values():
            for msg in messages:
                batch.append(msg.value.decode("utf-8"))
        return batch

    async def consume(self):
        """Основной цикл потребления сообщений"""
        last_batch_time = asyncio.get_event_loop().time()
        batch = []
        try:
            while True:
                try:
                    new_messages = await self.get_batch()
                    batch.extend(new_messages)
                    current_time = asyncio.get_event_loop().time()
                    time_elapsed = current_time - last_batch_time

                    if self.should_insert(batch, time_elapsed):
                        await self._process_batch(batch)
                        batch = []
                        last_batch_time = current_time

                        # Коммитим оффсеты после успешной вставки
                        await self.consumer.commit()

                except Exception as e:
                    logger.error(f"Error in consume loop: {e}", exc_info=True)
                    # Небольшая пауза перед retry
                    await asyncio.sleep(1)

        finally:
            # Обрабатываем оставшиеся сообщения при остановке
            if batch:
                await self._process_batch(batch)
                await self.consumer.commit()

    async def _process_batch(self, batch: list[str]):
        if not batch:
            return
        logger.info(f"Processing batch of {len(batch)} messages")
        try:
            success, errors = await self.db_processor.insert_batch(batch)

            if errors > 0:
                logger.warning(
                    f"Batch processed with errors: {success} success, {errors} errors"
                )
            else:
                logger.info(f"Batch processed successfully: {success} records")

        except Exception as e:
            logger.error(f"Failed to process batch: {e}", exc_info=True)
            raise


async def consume(
    host: str,
    port: int,
    topic: str,
    verbose: bool,
    group_id: str = "python-oltp-consumer",
    batch_size: int = 1000,
    batch_timeout: float = 5.0,
    database_url: str | None = None,
):
    consumer = KafkaConsumer(
        host=host,
        port=port,
        topic=topic,
        group_id=group_id,
        batch_size=batch_size,
        batch_timeout=batch_timeout,
        database_url=database_url,
    )
    try:
        await consumer.start()
        await consumer.consume()
    finally:
        await consumer.stop()
