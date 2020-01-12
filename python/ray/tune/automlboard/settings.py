"""
Django settings for monitor project.

Generated by 'django-admin startproject' using Django 1.11.14.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.11/ref/settings/
"""

import os

# Build paths inside the project like this:
# os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# You can specify your own secret key, here we just pick one randomly.
SECRET_KEY = "tktks103=$7a#5axn)52&b87!#w_qm(%*72^@hsq!nur%dtk4b"

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ["*"]

# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "ray.tune.automlboard.models",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "ray.tune.automlboard.frontend.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR + "/templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "ray.tune.automlboard.frontend.wsgi.application"

DB_ENGINE_NAME_MAP = {
    "mysql": "django.db.backends.mysql",
    "sqllite": "django.db.backends.sqlite3"
}


def lookup_db_engine(name):
    """Lookup db engine class name for engine name."""
    return DB_ENGINE_NAME_MAP.get(name, DB_ENGINE_NAME_MAP["sqllite"])


# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases
if not os.environ.get("AUTOMLBOARD_DB_ENGINE", None):
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": "automlboard.db",
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": lookup_db_engine(os.environ["AUTOMLBOARD_DB_ENGINE"]),
            "NAME": os.environ["AUTOMLBOARD_DB_NAME"],
            "USER": os.environ["AUTOMLBOARD_DB_USER"],
            "PASSWORD": os.environ["AUTOMLBOARD_DB_PASSWORD"],
            "HOST": os.environ["AUTOMLBOARD_DB_HOST"],
            "PORT": os.environ["AUTOMLBOARD_DB_PORT"]
        }
    }

# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

VALIDATION_PREFIX = "django.contrib.auth.password_validation."

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": VALIDATION_PREFIX + "UserAttributeSimilarityValidator",
    },
    {
        "NAME": VALIDATION_PREFIX + "MinimumLengthValidator",
    },
    {
        "NAME": VALIDATION_PREFIX + "CommonPasswordValidator",
    },
    {
        "NAME": VALIDATION_PREFIX + "NumericPasswordValidator",
    },
]

# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "Asia/Shanghai"

USE_I18N = True

USE_L10N = True

USE_TZ = False

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.11/howto/static-files/

STATIC_URL = "/static/"
STATICFILES_DIRS = (os.path.join(BASE_DIR, "static").replace("\\", "/"), )

# automlboard settings
AUTOMLBOARD_LOG_DIR = os.environ.get("AUTOMLBOARD_LOGDIR", None)
AUTOMLBOARD_RELOAD_INTERVAL = os.environ.get("AUTOMLBOARD_RELOAD_INTERVAL",
                                             None)
AUTOMLBOARD_LOG_LEVEL = os.environ.get("AUTOMLBOARD_LOGLEVEL", None)
