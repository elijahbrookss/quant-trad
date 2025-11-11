#!/usr/bin/env python3
"""Generate PR changelog from git diff using Ollama."""
import os
import sys
import argparse
from pathlib import Path
from typing import Optional
import yaml
from ollama import chat


def load_config(config_path: Path) -> dict:
    """Load prompts and settings from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
        print("Please create config/prompts.yaml", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"ERROR: Invalid YAML in config: {e}", file=sys.stderr)
        sys.exit(1)


def build_prompts(config: dict, diff_content: str) -> tuple[str, str]:
    """Build system and user prompts from config and diff content."""
    system_prompt = config['system_prompt'].strip()

    max_chars = config.get('max_diff_chars', 5000)
    truncated_diff = diff_content[:max_chars]

    truncation_notice = ""
    if len(diff_content) > max_chars:
        truncation_notice = f"...(diff truncated, showing first {max_chars} chars)"

    user_prompt = config['user_prompt_template'].format(
        diff_content=truncated_diff,
        truncation_notice=truncation_notice
    ).strip()

    return system_prompt, user_prompt


def generate_changelog(
    diff_path: Path,
    output_path: Path,
    config_path: Path,
    model: str,
    endpoint: Optional[str] = None
) -> int:
    """Generate changelog from git diff using LLM."""

    try:
        # Load configuration
        config = load_config(config_path)

        # Read diff file
        print(f"Reading diff from: {diff_path}", file=sys.stderr)

        if not diff_path.exists():
            print(f"ERROR: Diff file not found: {diff_path}", file=sys.stderr)
            return 1

        diff_content = diff_path.read_text()

        if not diff_content.strip():
            print("ERROR: Diff file is empty", file=sys.stderr)
            return 1

        print(f"Diff size: {len(diff_content)} characters", file=sys.stderr)

        # Build prompts
        system_prompt, user_prompt = build_prompts(config, diff_content)

        # Call LLM
        print(f"Calling Ollama model: {model}", file=sys.stderr)

        response = chat(
            model=model,
            messages=[
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': user_prompt}
            ],
            options=config.get('model_config', {})
        )

        changelog = response['message']['content'].strip()

        # Write output
        print(f"Writing changelog to: {output_path}", file=sys.stderr)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(changelog)

        print(f"Success: Generated changelog ({len(changelog)} chars)", file=sys.stderr)
        print(f"\nPreview:\n{changelog[:200]}...\n", file=sys.stderr)

        return 0

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


def main():
    parser = argparse.ArgumentParser(
        description="Generate PR changelog from git diff using Ollama"
    )
    parser.add_argument(
        '--diff',
        type=Path,
        required=True,
        help='Path to git diff file'
    )
    parser.add_argument(
        '--out',
        type=Path,
        required=True,
        help='Output path for changelog'
    )
    parser.add_argument(
        '--config',
        type=Path,
        default=Path(__file__).parent / 'config' / 'prompts.yaml',
        help='Path to prompts config (default: config/prompts.yaml)'
    )
    parser.add_argument(
        '--model',
        default=os.getenv('MODEL_NAME', 'llama3:8b'),
        help='Ollama model to use (default: llama3:8b)'
    )
    parser.add_argument(
        '--endpoint',
        default=os.getenv('LLM_ENDPOINT'),
        help='Ollama endpoint URL'
    )

    args = parser.parse_args()

    sys.exit(generate_changelog(
        args.diff,
        args.out,
        args.config,
        args.model,
        args.endpoint
    ))


if __name__ == "__main__":
    main()
