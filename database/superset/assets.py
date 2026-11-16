from bundle import bundle
from superset.app import create_app


def main() -> None:
    with create_app().app_context():
        from superset.commands.importers.v1.examples import ImportExamplesCommand

        ImportExamplesCommand(bundle(), overwrite=True).run()


if __name__ == "__main__":
    main()
