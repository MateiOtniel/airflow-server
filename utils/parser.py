import argparse


def parse_args(args: list[str]):
    parser = argparse.ArgumentParser()
    for arg in args:
        parser.add_argument(arg, required = True)
    return parser.parse_args()