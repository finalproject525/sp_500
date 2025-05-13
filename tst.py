import os

def print_tree(directory, prefix=""):
    items = sorted(os.listdir(directory))
    items = [i for i in items if not i.startswith('.') and i != '__pycache__']

    for index, item in enumerate(items):
        path = os.path.join(directory, item)
        is_last = index == len(items) - 1
        connector = "└── " if is_last else "├── "
        print(prefix + connector + item)
        if os.path.isdir(path):
            extension = "    " if is_last else "│   "
            print_tree(path, prefix + extension)

if __name__ == "__main__":
    print(f"📁 Project structure: {os.getcwd()}\n")
    print_tree(".")
