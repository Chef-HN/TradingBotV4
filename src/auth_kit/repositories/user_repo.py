from typing import Type, TypeVar
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from auth_kit.auth.passwords import hash_password

T = TypeVar("T")


async def create_user(
    db: AsyncSession,
    model_class: Type[T],
    email: str,
    password: str,
    display_name: str = "",
    preferred_locale: str = "es",
) -> T:
    user = model_class(
        email=email,
        password_hash=hash_password(password),
        display_name=display_name,
        preferred_locale=preferred_locale,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def get_user_by_email(db: AsyncSession, model_class: Type[T], email: str) -> T | None:
    result = await db.execute(select(model_class).where(func.lower(model_class.email) == email.lower()))
    return result.scalar_one_or_none()


async def get_user_by_id(db: AsyncSession, model_class: Type[T], user_id: str) -> T | None:
    result = await db.execute(select(model_class).where(model_class.id == user_id))
    return result.scalar_one_or_none()


async def delete_user(db: AsyncSession, user) -> None:
    await db.delete(user)
    await db.commit()


async def update_password(db: AsyncSession, user, new_password: str) -> None:
    """Update the user's password hash."""
    user.password_hash = hash_password(new_password)
    await db.commit()
