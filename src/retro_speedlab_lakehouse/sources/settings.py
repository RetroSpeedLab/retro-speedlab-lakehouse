from pydantic import BaseModel, ConfigDict

from retro_speedlab_lakehouse.environment import secret


class SourceSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class GithubSettings(SourceSettings):
    owner: str
    repositories: list[str]
    token: str


class UptimeSettings(SourceSettings):
    url: str
    page: str


class TwitchSettings(SourceSettings):
    client_id: str
    client_secret: str
    channel: str


class DiscordSettings(SourceSettings):
    token: str
    channel: str


class UmamiSettings(SourceSettings):
    url: str
    username: str
    password: str
    website: str


class LangfuseSettings(SourceSettings):
    url: str
    public_key: str
    secret_key: str


class WandbSettings(SourceSettings):
    api_key: str
    project: str


class BugsinkSettings(SourceSettings):
    url: str
    token: str


class Sources(SourceSettings):
    github: GithubSettings
    uptime: UptimeSettings
    twitch: TwitchSettings
    discord: DiscordSettings
    umami: UmamiSettings
    langfuse: LangfuseSettings
    wandb: WandbSettings
    bugsink: BugsinkSettings


def load_sources(github: GithubSettings, uptime: UptimeSettings) -> Sources:
    return Sources(
        github=github,
        uptime=uptime,
        twitch=TwitchSettings(
            client_id=secret("TWITCH_CLIENT_ID"),
            client_secret=secret("TWITCH_CLIENT_SECRET"),
            channel=secret("TWITCH_CHANNEL"),
        ),
        discord=DiscordSettings(token=secret("DISCORD_BOT_TOKEN"), channel=secret("DISCORD_RECORDS_CHANNEL")),
        umami=UmamiSettings(
            url=secret("UMAMI_API_BASE"),
            username=secret("UMAMI_USERNAME"),
            password=secret("UMAMI_PASSWORD"),
            website=secret("UMAMI_WEBSITE_ID"),
        ),
        langfuse=LangfuseSettings(
            url=secret("LANGFUSE_BASE_URL"),
            public_key=secret("LANGFUSE_PUBLIC_KEY"),
            secret_key=secret("LANGFUSE_SECRET_KEY"),
        ),
        wandb=WandbSettings(api_key=secret("WANDB_API_KEY"), project=secret("WANDB_PROJECT")),
        bugsink=BugsinkSettings(url=secret("BUGSINK_API_BASE"), token=secret("BUGSINK_API_TOKEN")),
    )
