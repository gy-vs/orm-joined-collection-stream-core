from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class _FixtureMixin:
    @classmethod
    def setup_classes(cls):
        class Base(cls.Comparable):
            pass

        class User(Base):
            pass

        class Address(Base):
            pass

        class Role(Base):
            pass

    @classmethod
    def setup_mappers(cls):
        User, Address, Role = cls.classes("User", "Address", "Role")

        cls.mapper_registry.map_imperatively(
            User,
            cls.tables.users,
            properties={
                "addresses": relationship(
                    Address,
                    back_populates="user",
                    order_by=cls.tables.addresses.c.id,
                ),
                "roles": relationship(Role, back_populates="user"),
            },
        )
        cls.mapper_registry.map_imperatively(
            Address,
            cls.tables.addresses,
            properties={
                "user": relationship(User, back_populates="addresses")
            },
        )
        cls.mapper_registry.map_imperatively(
            Role,
            cls.tables.roles,
            properties={"user": relationship(User, back_populates="roles")},
        )

    @classmethod
    def define_tables(cls, metadata):
        users = cls.tables.users = cls._users_table(metadata)
        cls.tables.addresses = cls._addresses_table(metadata, users)
        cls.tables.roles = cls._roles_table(metadata, users)

    @staticmethod
    def _users_table(metadata):
        return Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

    @staticmethod
    def _addresses_table(metadata, users):
        return Table(
            "addresses",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("email", String(50)),
            Column("user_id", ForeignKey("users.id")),
        )

    @staticmethod
    def _roles_table(metadata, users):
        return Table(
            "roles",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("user_id", ForeignKey("users.id")),
        )

    @classmethod
    def insert_data(cls, connection):
        User, Address, Role = cls.classes("User", "Address", "Role")

        with Session(connection) as session:
            # user counts of children deliberately vary, including a user
            # with more children than any streaming batch size below
            for i in range(1, 9):
                user = User(id=i, name=f"user_{i}")
                user.addresses = [
                    Address(id=(i - 1) * 100 + j, email=f"{i}-{j}")
                    for j in range(1, 1 + ((i * 3) % 7))
                ]
                user.roles = [
                    Role(id=(i - 1) * 100 + j, name=f"role_{j}")
                    for j in (1, 2)
                ]
                session.add(user)
            session.commit()

    def _buffered(self, options):
        User = self.classes.User
        sess = fixture_session()
        stmt = select(User).options(*options).order_by(User.id)
        return [
            (
                u.id,
                [a.email for a in u.addresses],
                [r.name for r in u.roles],
            )
            for u in sess.execute(stmt).unique().scalars()
        ]


class StreamJoinedCollectionTest(_FixtureMixin, fixtures.MappedTest):
    __sparse_driver_backend__ = True

    @testing.combinations((1,), (2,), (3,), (5,), (50,), argnames="yield_per")
    def test_iter_matches_buffered(self, yield_per):
        User = self.classes.User
        expected = self._buffered([joinedload(User.addresses)])

        sess = fixture_session()
        stmt = (
            select(User)
            .options(joinedload(User.addresses))
            .order_by(User.id)
            .execution_options(yield_per=yield_per)
        )
        result = sess.execute(stmt)
        got = [
            (u.id, [a.email for a in u.addresses])
            for u in result.scalars()
        ]
        eq_(
            got,
            [(uid, addrs) for uid, addrs, roles in expected],
        )

    @testing.combinations((1,), (2,), (3,), (5,), (50,), argnames="yield_per")
    def test_fetchmany_matches_buffered(self, yield_per):
        User = self.classes.User
        expected = self._buffered([joinedload(User.addresses)])

        sess = fixture_session()
        stmt = (
            select(User)
            .options(joinedload(User.addresses))
            .order_by(User.id)
            .execution_options(yield_per=yield_per)
        )
        result = sess.execute(stmt)

        got = []
        while True:
            rows = result.fetchmany(2)
            if not rows:
                break
            got.extend(
                (u.id, [a.email for a in u.addresses]) for u, in rows
            )
        eq_(got, [(uid, addrs) for uid, addrs, roles in expected])

    @testing.combinations((1,), (2,), (3,), (5,), (50,), argnames="yield_per")
    def test_all_matches_buffered(self, yield_per):
        User = self.classes.User
        expected = self._buffered([joinedload(User.addresses)])

        sess = fixture_session()
        stmt = (
            select(User)
            .options(joinedload(User.addresses))
            .order_by(User.id)
            .execution_options(yield_per=yield_per)
        )
        result = sess.execute(stmt)
        got = [
            (u.id, [a.email for a in u.addresses]) for u in result.scalars()
        ]
        eq_(got, [(uid, addrs) for uid, addrs, roles in expected])

    def test_collection_complete_when_larger_than_batch(self):
        User = self.classes.User
        # user 7 has the largest collection: (7 * 3) % 7 == 0 -> 6 children.
        # build an additional user with more children than the batch size.
        Address = self.classes.Address
        sess = fixture_session(bind=testing.db)
        big = User(id=100, name="big")
        big.addresses = [
            Address(id=10000 + j, email=f"big-{j}") for j in range(25)
        ]
        sess.add(big)
        sess.commit()
        sess.close()

        sess = fixture_session()
        stmt = (
            select(User)
            .options(joinedload(User.addresses))
            .order_by(User.id)
            .execution_options(yield_per=5)
        )
        users = sess.execute(stmt).scalars().all()
        loaded = {u.id: [a.email for a in u.addresses] for u in users}
        eq_(len(loaded[100]), 25)
        eq_(loaded[100], [f"big-{j}" for j in range(25)])
        # parent appears exactly once
        eq_(len([uid for uid in loaded if uid == 100]), 1)

    def test_each_parent_seen_once(self):
        User = self.classes.User

        sess = fixture_session()
        stmt = (
            select(User)
            .options(joinedload(User.addresses))
            .order_by(User.id)
            .execution_options(yield_per=2)
        )
        ids = [u.id for u in sess.execute(stmt).scalars()]
        eq_(ids, sorted(ids))
        eq_(len(ids), len(set(ids)))

    def test_non_contiguous_ordering_rejected(self):
        User, Address = self.classes("User", "Address")

        sess = fixture_session(bind=testing.db)
        # make the child ordering interleave lead parents
        emails = {}
        for i in range(1, 9):
            for j, addr in enumerate(
                sess.get(User, i).addresses, start=1
            ):
                emails[addr.id] = f"{(i + (j * 3)) % 9:02d}-{j}"
        for addr in sess.query(Address):
            addr.email = emails[addr.id]
        sess.commit()
        sess.close()

        sess = fixture_session()
        stmt = (
            select(User)
            .options(joinedload(User.addresses))
            .join(User.addresses)
            .order_by(Address.email)
            .execution_options(yield_per=2)
        )
        assert_raises_message(
            exc.InvalidRequestError,
            "not grouped contiguously by identity",
            lambda: sess.execute(stmt).scalars().all(),
        )

    @testing.combinations((1,), (3,), (10,), argnames="yield_per")
    def test_selectinload_on_lead_still_fires(self, yield_per):
        User = self.classes.User
        expected = self._buffered(
            [joinedload(User.addresses), selectinload(User.roles)]
        )

        sess = fixture_session()
        stmt = (
            select(User)
            .options(
                joinedload(User.addresses), selectinload(User.roles)
            )
            .order_by(User.id)
            .execution_options(yield_per=yield_per)
        )
        got = [
            (u.id, [a.email for a in u.addresses], [r.name for r in u.roles])
            for u in sess.execute(stmt).scalars()
        ]
        eq_(got, expected)

    @testing.combinations((1,), (3,), (10,), argnames="yield_per")
    def test_selectinload_for_collection_only(self, yield_per):
        # selectinload() on a collection continues to work with yield_per
        # with no joined collection present (pre-existing path)
        User = self.classes.User
        expected = self._buffered([selectinload(User.addresses)])

        sess = fixture_session()
        stmt = (
            select(User)
            .options(selectinload(User.addresses))
            .order_by(User.id)
            .execution_options(yield_per=yield_per)
        )
        got = [
            (u.id, [a.email for a in u.addresses])
            for u in sess.execute(stmt).scalars()
        ]
        eq_(got, [(uid, addrs) for uid, addrs, roles in expected])

    def test_subqueryload_still_rejected(self):
        User = self.classes.User
        sess = fixture_session()
        stmt = (
            select(User)
            .options(subqueryload(User.addresses))
            .execution_options(yield_per=10)
        )
        assert_raises_message(
            exc.InvalidRequestError,
            "Can't use yield_per with eager loaders that require row "
            "buffering",
            lambda: sess.execute(stmt).all(),
        )

    @testing.combinations((1,), (2,), (5,), argnames="yield_per")
    def test_contains_eager_collection(self, yield_per):
        User, Address = self.classes("User", "Address")
        sess = fixture_session()
        stmt = (
            select(User)
            .join(User.addresses)
            .options(contains_eager(User.addresses))
            .order_by(User.id, Address.id)
            .execution_options(yield_per=yield_per)
        )
        got = [
            (u.id, [a.email for a in u.addresses])
            for u in sess.execute(stmt).scalars()
        ]
        expected = self._buffered([joinedload(User.addresses)])
        eq_(
            got,
            [(uid, addrs) for uid, addrs, roles in expected if addrs],
        )

    def test_legacy_query(self):
        User = self.classes.User
        expected = self._buffered([joinedload(User.addresses)])

        sess = fixture_session()
        q = (
            sess.query(User)
            .options(joinedload(User.addresses))
            .order_by(User.id)
            .yield_per(2)
        )
        got = [(u.id, [a.email for a in u.addresses]) for u in q]
        eq_(got, [(uid, addrs) for uid, addrs, roles in expected])

    def test_explicit_unique_is_idempotent(self):
        User = self.classes.User
        expected = self._buffered([joinedload(User.addresses)])

        sess = fixture_session()
        stmt = (
            select(User)
            .options(joinedload(User.addresses))
            .order_by(User.id)
            .execution_options(yield_per=3)
        )
        got = [
            (u.id, [a.email for a in u.addresses])
            for u in sess.execute(stmt).unique().scalars()
        ]
        eq_(got, [(uid, addrs) for uid, addrs, roles in expected])

    def test_partial_consumption_and_close(self):
        User = self.classes.User
        sess = fixture_session()
        stmt = (
            select(User)
            .options(joinedload(User.addresses))
            .order_by(User.id)
            .execution_options(yield_per=1)
        )
        result = sess.execute(stmt)
        it = iter(result.scalars())
        first = next(it)
        eq_(first.id, 1)
        # collection must already be complete
        eq_([a.email for a in first.addresses], ["1-1", "1-2", "1-3"])
        result.close()
