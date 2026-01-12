import os

def find_repo_root(start_dir=None) -> str:
    """
    Find the root of a GitHub repository by searching for a `.git` folder.

    :param start_dir: The directory to start the search from.
    Defaults to the current working directory.

    Returns: the path to the repository root, or None if not found.
    """
    # If no start directory is provided, use the current working directory
    if start_dir is None:
        start_dir = os.getcwd()

    current_dir = start_dir

    while True:
        # Check if `.git` exists in the current directory
        if os.path.isdir(os.path.join(current_dir, '.git')):
            return current_dir  # Found the repo root.

        # Move up one directory level
        parent_dir = os.path.dirname(current_dir)

        # If we reach the root directory, stop (no more parents to search)
        if current_dir == parent_dir:
            return None  # Repo root not found

        current_dir = parent_dir