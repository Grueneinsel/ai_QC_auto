#!/usr/bin/env python3
from pathlib import Path

def write_from_config(
    input_value: str,
    output_value: str,
    inner_folder: str,                   # Pflicht
    template_filename: str = "mcquac.json",
    config_dir: str = "config",
    encoding: str = "utf-8",
) -> Path:
    """
    Liest ./config/<template_filename>, ersetzt %%%INPUT%%% und %%%OUTPUT%%%
    und schreibt nach ./tmp/<inner_folder>/ mit gleichem Dateinamen.
    """
    if not inner_folder or not str(inner_folder).strip():
        raise ValueError("inner_folder muss angegeben werden und darf nicht leer sein.")

    root = Path.cwd()
    template_path = root / config_dir / template_filename
    if not template_path.is_file():
        raise FileNotFoundError(f"Vorlage nicht gefunden: {template_path}")

    text = template_path.read_text(encoding=encoding)
    text = text.replace("%%%INPUT%%%", str(input_value)).replace("%%%OUTPUT%%%", str(output_value))

    out_dir = root / "tmp" / inner_folder
    out_dir.mkdir(parents=True, exist_ok=True)

    target = out_dir / template_path.name
    target.write_text(text, encoding=encoding)
    return target
