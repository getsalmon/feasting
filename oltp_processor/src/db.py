import datetime as dt
import json
import logging
from dataclasses import dataclass

import prettyprinter as pp
from sqlalchemy import func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

import config
import data_types
from models import Brand, Category, Event, Product, Purchase, User

logger = logging.getLogger(__name__)


class DBProcessor:
    def __init__(self, database_url: str | None = None):
        if not database_url:
            database_url = config.build_pg_url()

        self.engine = create_async_engine(database_url, echo=False)
        self.async_session = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def close(self):
        await self.engine.dispose()

    @staticmethod
    def parse_records(records: list[str]) -> tuple[list[data_types.DatasetRow], int]:
        res = []
        error_count = 0
        for value in records:
            try:
                rec = json.loads(value)
                ds = data_types.DatasetRow.from_dict(rec)
                res.append(ds)
            except Exception as e:
                logger.error(f"Failed to parse record: {e}")
                error_count += 1
        return res, error_count

    async def insert_batch(self, records: list[str]):
        try:
            if not records:
                return 0, 0
            parsed_records, parsing_errors_cnt = self.parse_records(records)
            if not parsed_records:
                return 0, parsing_errors_cnt

            async with self.async_session() as session:
                await self.upsert_users(session, parsed_records)
                await self.upsert_categories(session, parsed_records)
                brand_mapping = await self.upsert_brands(session, parsed_records)

                await self.upsert_products(session, parsed_records, brand_mapping)

                events, purchases = self.prepare_events_and_purchases(parsed_records)
                await self.insert_events(session, events)
                await self.insert_purchases(session, purchases)

                await session.commit()
                success_cnt = len(parsed_records)
                logger.info(f"Successfully inserted {success_cnt} records")
                return success_cnt, parsing_errors_cnt
        except Exception as e:
            logger.error(f"Error during batch insert: {e}", exc_info=True)
            return 0, parsing_errors_cnt + len(parsed_records)

    async def upsert_users(
        self, session: AsyncSession, records: list[data_types.DatasetRow]
    ):
        user_ids = {r.user_id for r in records}
        if not user_ids:
            return
        stmt = (
            pg_insert(User)
            .values([{"user_id": user_id} for user_id in user_ids])
            .on_conflict_do_nothing(index_elements=["user_id"])
        )
        await session.execute(stmt)
        logger.debug(f"Upserted {len(user_ids)} users")

    async def upsert_categories(
        self, session: AsyncSession, records: list[data_types.DatasetRow]
    ):
        categories = {
            r.category_id: r.category_code if r.category_code != "" else None
            for r in records
        }
        if not categories:
            return
        stmt = pg_insert(Category).values(
            [
                {"category_id": cid, "category_code": code}
                for cid, code in categories.items()
            ]
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["category_id"],
            set_={
                "category_code": func.coalesce(
                    stmt.excluded.category_code,
                    Category.category_code,
                )
            },
        )

        await session.execute(stmt)
        logger.debug(f"Upserted {len(categories)} categories")

    async def upsert_brands(
        self, session: AsyncSession, records: list[data_types.DatasetRow]
    ) -> dict[str, int]:
        brands = {r.brand for r in records if r.brand}
        if not brands:
            return {}

        stmt = (
            pg_insert(Brand)
            .values([{"brand_name": name} for name in brands])
            .on_conflict_do_nothing(index_elements=["brand_name"])
        )
        await session.execute(stmt)
        stmt = select(Brand.brand_name, Brand.brand_id).where(
            Brand.brand_name.in_(brands)
        )
        result = await session.execute(stmt)
        brand_mapping = dict(result.all())

        logger.debug(f"Loaded {len(brand_mapping)} brand mappings")
        return brand_mapping

    async def upsert_products(
        self,
        session: AsyncSession,
        records: list[data_types.DatasetRow],
        brand_mapping: dict[str, int],
    ) -> list[dict]:
        products = {}
        for r in records:
            brand_id = brand_mapping.get(r.brand) if r.brand else None
            products[r.product_id] = {
                "product_id": r.product_id,
                "category_id": r.category_id,
                "brand_id": brand_id,
            }
        if not products:
            return

        stmt = pg_insert(Product).values(list(products.values()))
        stmt = stmt.on_conflict_do_update(
            index_elements=["product_id"],
            set_={
                "category_id": func.coalesce(
                    stmt.excluded.category_id,
                    Product.category_id,
                ),
                "brand_id": func.coalesce(
                    stmt.excluded.brand_id,
                    Product.brand_id,
                ),
            },
        )
        await session.execute(stmt)
        logger.debug(f"Upserted {len(products)} products")

    def prepare_events_and_purchases(
        self, records: list[data_types.DatasetRow]
    ) -> tuple[list[dict], list[dict]]:
        events = []
        purchases = []

        for r in records:
            data = {
                "id": r.row_id,
                "event_time": r.event_time,
                "product_id": r.product_id,
                "price": r.price,
                "user_id": r.user_id,
                "user_session": r.user_session,
            }

            if r.event_type == "purchase":
                purchases.append(data)
            else:
                events.append({**data, "event_type": r.event_type})
        return events, purchases

    async def insert_events(self, session: AsyncSession, events: list[dict]):
        if not events:
            return
        stmt = (
            pg_insert(Event)
            .values(events)
            .on_conflict_do_nothing(index_elements=["id"])
        )
        await session.execute(stmt)
        logger.debug(f"Inserted {len(events)} events")

    async def insert_purchases(self, session: AsyncSession, purchases: list[dict]):
        if not purchases:
            return
        stmt = (
            pg_insert(Purchase)
            .values(purchases)
            .on_conflict_do_nothing(index_elements=["id"])
        )
        await session.execute(stmt)
        logger.debug(f"Inserted {len(purchases)} purchases")
