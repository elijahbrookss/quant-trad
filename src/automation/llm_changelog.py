#!/usr/bin/env python3
"""Generate changelog and create Notion release entry from git diff using Ollama."""

import sys
import argparse
import subprocess
import yaml
from datetime import date
from pathlib import Path
from typing import Dict

# Import the Notion client function
from notion.notion_client import create_release_page


def load_config(config_path: str) -> Dict:
    """Load prompts and settings from YAML config file.
    
    Args:
        config_path: Path to the YAML config file
        
    Returns:
        Dictionary containing config settings including prompts
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file has invalid YAML
    """
    path = Path(config_path)
    
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            f"Please create the config file with system_prompt and user_prompt_template."
        )
    
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in config file: {e}") from e
    
    # Validate required fields
    required_fields = ['system_prompt', 'user_prompt_template']
    for field in required_fields:
        if field not in config:
            raise ValueError(
                f"Config file missing required field: '{field}'\n"
                f"Config must contain: {', '.join(required_fields)}"
            )
    
    return config


def read_diff(diff_path: str) -> str:
    """Read a diff file from disk and return it as a string.
    
    Args:
        diff_path: Path to the diff file
        
    Returns:
        The diff content as a string
        
    Raises:
        FileNotFoundError: If the diff file doesn't exist
        ValueError: If the diff file is empty
    """
    path = Path(diff_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Diff file not found: {diff_path}")
    
    content = path.read_text()
    
    if not content.strip():
        raise ValueError(f"Diff file is empty: {diff_path}")
    
    return content


def call_ollama(model: str, system_prompt: str, user_prompt: str) -> str:
    """Call Ollama CLI with system and user prompts and return the response.
    
    Uses subprocess to invoke the ollama CLI command with a combined prompt.
    
    Args:
        model: The Ollama model name (e.g., "llama3.2)
        system_prompt: The system-level instructions for the model
        user_prompt: The user query/request with the diff content
        
    Returns:
        The model's response as a string
        
    Raises:
        RuntimeError: If the ollama command fails or there's an error
    """
    # Combine system and user prompts for the CLI
    # Format: System instructions followed by the user request
    combined_prompt = f"{system_prompt}\n\n---\n\n{user_prompt}"
    
    try:
        result = subprocess.run(
            ["ollama", "run", model],
            input=combined_prompt,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Ollama command failed with exit code {e.returncode}:\n"
            f"stderr: {e.stderr}"
        ) from e
    except Exception as e:
        raise RuntimeError(f"Error calling Ollama: {e}") from e


def build_prompt(diff_text: str, config: Dict) -> tuple[str, str]:
    """Build system and user prompts from config and diff content.
    
    Args:
        diff_text: The git diff content
        config: Configuration dictionary loaded from YAML
        
    Returns:
        A tuple of (system_prompt, user_prompt)
    """
    # Get max chars from config or use default
    max_chars = config.get('max_diff_chars', 8000)
    
    # Truncate diff if too long to avoid token limits
    truncated_diff = diff_text[:max_chars]
    truncation_notice = ""
    if len(diff_text) > max_chars:
        truncation_notice = f"\n\n(Note: Diff truncated to {max_chars} characters)"
    
    # Build prompts from config templates
    system_prompt = config['system_prompt'].strip()
    user_prompt = config['user_prompt_template'].format(
        diff_content=truncated_diff,
        truncation_notice=truncation_notice
    ).strip()
    
    return system_prompt, user_prompt


def parse_model_output(raw: str) -> Dict[str, str]:
    """Parse the structured text output from the model.
    
    Expects the model output to contain four sections marked with:
    - TLDR:
    - SUMMARY:
    - SOCIAL_POST:
    - DEV_POST:
    
    Args:
        raw: The raw text response from the model
        
    Returns:
        A dictionary with keys: 'tldr', 'summary', 'social_post', 'dev_post'
        
    Raises:
        ValueError: If the expected sections cannot be found or are empty
    """
    # Normalize line endings and strip
    raw = raw.strip()

    # DEBUG: Print what we received
    print("\n" + "="*60, file=sys.stderr)
    print("RAW MODEL OUTPUT:", file=sys.stderr)
    print("="*60, file=sys.stderr)
    print(raw, file=sys.stderr)
    print("="*60 + "\n", file=sys.stderr)
    
    # Define section markers
    tldr_marker = "TLDR:"
    summary_marker = "SUMMARY:"
    social_marker = "SOCIAL_POST:"
    dev_marker = "DEV_POST:"
    
    # Check all markers exist
    if tldr_marker not in raw:
        raise ValueError(
            f"Could not find '{tldr_marker}' section in model output. "
            f"Make sure the model returned the expected format."
        )
    if summary_marker not in raw:
        raise ValueError(
            f"Could not find '{summary_marker}' section in model output. "
            f"Make sure the model returned the expected format."
        )
    if social_marker not in raw:
        raise ValueError(
            f"Could not find '{social_marker}' section in model output. "
            f"Make sure the model returned the expected format."
        )
    if dev_marker not in raw:
        raise ValueError(
            f"Could not find '{dev_marker}' section in model output. "
            f"Make sure the model returned the expected format."
        )
    
    # Find positions of markers
    tldr_start = raw.index(tldr_marker) + len(tldr_marker)
    summary_start = raw.index(summary_marker)
    social_start = raw.index(social_marker)
    dev_start = raw.index(dev_marker)
    
    # Extract sections between markers
    tldr = raw[tldr_start:summary_start].strip()
    summary = raw[summary_start + len(summary_marker):social_start].strip()
    social_post = raw[social_start + len(social_marker):dev_start].strip()
    dev_post = raw[dev_start + len(dev_marker):].strip()
    
    # Validate we got content in each section
    if not tldr:
        raise ValueError("TLDR section is empty")
    if not summary:
        raise ValueError("Summary section is empty")
    if not social_post:
        raise ValueError("Social post section is empty")
    if not dev_post:
        raise ValueError("Dev post section is empty")
    
    return {
        "tldr": tldr,
        "summary": summary,
        "social_post": social_post,
        "dev_post": dev_post
    }


def orchestrate(
    diff_path: str,
    branch: str,
    release_name: str,
    config_path: str,
    model: str = "llama3.2",
    dry_run: bool = False
) -> Dict[str, str]:
    """End-to-end orchestrator for the changelog automation pipeline.
    
    Pipeline steps:
    1. Load configuration from YAML
    2. Read the diff file
    3. Build the prompts from config
    4. Call Ollama to generate content
    5. Parse the model output
    6. Create Notion release page (unless dry_run=True)
    
    Args:
        diff_path: Path to the diff file
        branch: Git branch name
        release_name: Human-friendly release name (e.g., "v0.0.5 - New features")
        config_path: Path to the YAML config file with prompts
        model: Ollama model to use (default: "llama3.2")
        dry_run: If True, skip the Notion API call and just return parsed results
        
    Returns:
        A dictionary containing:
        - release_name
        - tldr
        - summary
        - branch
        - social_post
        - dev_post
        - raw_model_output
        - notion_page_id (only if dry_run=False)
    """
    print(f"[1/6] Loading config from: {config_path}", file=sys.stderr)
    config = load_config(config_path)
    print(f"  ✓ Config loaded", file=sys.stderr)
    
    print(f"[2/6] Reading diff from: {diff_path}", file=sys.stderr)
    diff_text = read_diff(diff_path)
    print(f"  ✓ Read {len(diff_text)} characters", file=sys.stderr)
    
    print(f"[3/6] Building prompts from config...", file=sys.stderr)
    system_prompt, user_prompt = build_prompt(diff_text, config)
    print(f"  ✓ Prompts ready (system: {len(system_prompt)} chars, user: {len(user_prompt)} chars)", file=sys.stderr)
    
    print(f"[4/6] Calling Ollama model: {model}", file=sys.stderr)
    print(f"  (This may take a minute...)", file=sys.stderr)
    raw_output = call_ollama(model, system_prompt, user_prompt)
    print(f"  ✓ Received response ({len(raw_output)} characters)", file=sys.stderr)
    
    print(f"[5/6] Parsing model output...", file=sys.stderr)
    parsed = parse_model_output(raw_output)
    print(f"  ✓ Extracted tldr, summary, social_post, and dev_post", file=sys.stderr)
    
    # Build result dictionary
    result = {
        "release_name": release_name,
        "tldr": parsed["tldr"],
        "summary": parsed["summary"],
        "branch": branch,
        "social_post": parsed["social_post"],
        "dev_post": parsed["dev_post"],
        "raw_model_output": raw_output,
    }
    
    if dry_run:
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"[DRY RUN MODE] Would create Notion release with:", file=sys.stderr)
        print(f"{'='*60}", file=sys.stderr)
        print(f"Release name: {release_name}", file=sys.stderr)
        print(f"Branch: {branch}", file=sys.stderr)
        print(f"Release date: {date.today()}", file=sys.stderr)
        print(f"\nTLDR:\n{parsed['tldr']}\n", file=sys.stderr)
        print(f"Summary preview:\n{parsed['summary'][:150]}...\n", file=sys.stderr)
        print(f"Social post preview:\n{parsed['social_post'][:150]}...\n", file=sys.stderr)
        print(f"Dev post preview:\n{parsed['dev_post'][:150]}...\n", file=sys.stderr)
        print(f"{'='*60}\n", file=sys.stderr)
        return result
    
    print(f"[6/6] Creating Notion release page...", file=sys.stderr)
    notion_response = create_release_page(
        name=release_name,
        summary=parsed["tldr"],
        branch=branch,
        full_summary=parsed["summary"],
        release_date=date.today(),
        social_post=parsed["social_post"],
        dev_post=parsed["dev_post"]
    )
    
    result["notion_page_id"] = notion_response["id"]
    
    print(f"   Created Notion release page: {notion_response['id']}", file=sys.stderr)
    print(f"\n Pipeline complete!", file=sys.stderr)
    
    return result


def main() -> None:
    """CLI entry point for the changelog automation tool."""
    parser = argparse.ArgumentParser(
        description="Generate changelog from git diff and create Notion release entry",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  %(prog)s --diff-file changes.diff --branch main --release-name "v1.0.0 - Major release"
  %(prog)s --diff-file changes.diff --branch develop --release-name "v1.0.1 - Bug fixes" --dry-run
  %(prog)s --diff-file changes.diff --branch feature/new --release-name "v1.1.0" --model llama3.2
  %(prog)s --diff-file changes.diff --branch main --release-name "v1.0.0" --config custom_prompts.yaml
        """
    )
    parser.add_argument(
        "--diff-file",
        required=True,
        help="Path to the git diff file"
    )
    parser.add_argument(
        "--branch",
        required=True,
        help="Git branch name"
    )
    parser.add_argument(
        "--release-name",
        required=True,
        help='Human-friendly release name (e.g., "v0.0.5 - New features")'
    )
    parser.add_argument(
        "--config",
        default="src/automation/config/prompts.yaml",
        help="Path to YAML config file with prompts (default: src/automation/config/prompts.yaml)"
    )
    parser.add_argument(
        "--model",
        default="llama3.2",
        help="Ollama model to use (default: llama3.2)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline without calling Notion API (for testing)"
    )
    
    args = parser.parse_args()
    
    try:
        result = orchestrate(
            diff_path=args.diff_file,
            branch=args.branch,
            release_name=args.release_name,
            config_path=args.config,
            model=args.model,
            dry_run=args.dry_run
        )
        
        # Print summary to stdout (structured for potential parsing)
        print("\n" + "="*60)
        print("RESULT SUMMARY")
        print("="*60)
        print(f"Release: {result['release_name']}")
        print(f"Branch: {result['branch']}")
        print(f"\nTLDR:\n{result['tldr']}")
        if 'notion_page_id' in result:
            print(f"\nNotion Page ID: {result['notion_page_id']}")
        else:
            print("\nNotion Page: [DRY RUN - not created]")
        print("="*60 + "\n")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\n ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()