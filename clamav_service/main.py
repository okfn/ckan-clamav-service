import os

from ckanserviceprovider import web

from clamav_service import jobs

# check whether jobs have been imported properly
assert jobs.scan


def serve():
    web.init()
    web.app.run(web.app.config.get("HOST"), web.app.config.get("PORT"))


def serve_test():
    web.init()
    return web.app.test_client()


def main():
    import argparse

    argparser = argparse.ArgumentParser(
        description="Service for scanning CKAN resource uploads with Clam AV", epilog=""
    )

    argparser.add_argument(
        "config",
        metavar="CONFIG",
        type=argparse.FileType("r"),
        help="configuration file",
    )
    args = argparser.parse_args()

    os.environ["JOB_CONFIG"] = os.path.abspath(args.config.name)
    serve()


if __name__ == "__main__":
    main()
