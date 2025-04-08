from main import noise_maker
from fastapi import HTTPException
from app_settings import create_app
from fastapi.middleware.cors import CORSMiddleware
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

app = create_app(create_custom_static_urls=True)

app.add_middleware(
    middleware_class=CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post(
    path='/noise',
    name='make_noise'
)
def make_noise(count_streets_update: int):
    try:
        noise_maker(count_streets_update=count_streets_update)
        return {'response': 'данные обновлены'}
    except Exception as e:
        raise HTTPException(status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=str(e))
