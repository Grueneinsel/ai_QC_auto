#!/usr/bin/env python3
from pathlib import Path


def write_from_config(
    input_value: str,
    output_value: str,
    inner_folder: str,                   # required
    template_filename: str = "mcquac.json",
    config_dir: str = "config",
    encoding: str = "utf-8",
) -> Path:
    """
    Read ./config/<template_filename>, replace %%%INPUT%%% and %%%OUTPUT%%%
    and write it to ./tmp/<inner_folder>/ with the same file name.
    """
    if not inner_folder or not str(inner_folder).strip():
        raise ValueError("inner_folder must be provided and must not be empty.")

    root = Path.cwd()
    template_path = root / config_dir / template_filename
    if not template_path.is_file():
        raise FileNotFoundError(f"Template not found: {template_path}")

    text = template_path.read_text(encoding=encoding)
    text = text.replace("%%%INPUT%%%", str(input_value)).replace("%%%OUTPUT%%%", str(output_value))

    out_dir = root / "tmp" / inner_folder
    out_dir.mkdir(parents=True, exist_ok=True)

    target = out_dir / template_path.name
    target.write_text(text, encoding=encoding)
    return target
