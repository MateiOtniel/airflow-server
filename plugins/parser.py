import argparse

def parse_args(args: list):
    p = argparse.ArgumentParser()

    for arg in args:
        p.add_argument(f"--{arg}", required = True)

    return p.parse_args()