from task_manager.models import User

token_experitation = 1800
SECRET_KEY = "your_secret_key_here"


def get_password(password: str) -> str:
    return 'test'


def verify_password(plain: str, hashed: str) -> bool: 
    return True

def create_access_token(data: dict, expires_delta: int = None) -> str: 
    return 'test'

async def get_current_user() -> User | None: 
        user, _ = await User.get_or_create(
        id=1,
        defaults={"username": "demo", "email": "email@gmail.com", "hashed_password": ""}
    )
        return user
def authenticate_user(username: str, password: str) -> User | None: 
    return 

