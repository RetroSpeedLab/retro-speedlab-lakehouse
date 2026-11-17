import os

from superset.app import create_app

OWNER = "owner"
OWNER_EMAIL = "owner@retrospeedlab.com"
ADMIN_ROLE = "Admin"


def main() -> None:
    with create_app().app_context():
        from superset import security_manager

        password = os.environ["SUPERSET_ADMIN_PASSWORD"]
        owner = security_manager.find_user(username=OWNER)
        if owner is None:
            security_manager.add_user(
                OWNER, "Owner", "Owner", OWNER_EMAIL, security_manager.find_role(ADMIN_ROLE), password=password
            )
            return
        security_manager.reset_password(owner.id, password)


if __name__ == "__main__":
    main()
