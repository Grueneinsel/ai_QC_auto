#!/usr/bin/env python3
from src.load_config import load_config

def main() -> None:
    cfg = load_config()
    print(f"Interval: {cfg.interval_minutes} min ({cfg.interval_seconds} s)")
    print(f"mcquac :  {cfg.mcquac_path}")
    print(f"default:  {cfg.default_pattern}")
    print("Pairs:")
    for pair in cfg.io_pairs:
        print(f"  IN : {pair.input}\n  OUT: {pair.output}\n  PAT: {pair.pattern}\n")

if __name__ == "__main__":
    main()
