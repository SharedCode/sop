import sys
import importlib
import pkgutil
import os
from . import examples

def list_examples():
    print("Available examples:")
    # Iterate over modules in the examples package
    for _, name, _ in pkgutil.iter_modules(examples.__path__):
        # Filter out internal files or data directories if any
        if not name.startswith('_') and not name.startswith('data'):
            print(f"  - {name}")

def run_example(name):
    try:
        # Import the module dynamically
        module_name = f"sop.examples.{name}"
        print(f"Loading {module_name}...")
        module = importlib.import_module(module_name)
        
        # Check if it has a main function
        if hasattr(module, "main"):
            print(f"--- Running {name} ---")
            module.main()
            print(f"--- Finished {name} ---")
        else:
            print(f"Example {name} does not have a main() function.")
            # Fallback: maybe it runs on import? (Bad practice but possible)
            
    except ImportError:
        print(f"Example '{name}' not found.")
        print("Use 'sop-demo list' to see available examples.")
    except Exception as e:
        print(f"Error running example: {e}")
        import traceback
        traceback.print_exc()

def main():
    if len(sys.argv) < 2:
        print("SOP Examples Runner")
        print("Usage:")
        print("  sop-demo list           # List all available examples")
        print("  sop-demo run <name>     # Run a specific example")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "list":
        list_examples()
    elif command == "run":
        if len(sys.argv) < 3:
            print("Error: Please specify an example name.")
            list_examples()
            sys.exit(1)
        run_example(sys.argv[2])
    else:
        print(f"Unknown command: {command}")
        print("Usage: sop-demo [list | run <example_name>]")

if __name__ == "__main__":
    main()
