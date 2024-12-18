import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import json
import os
import sys
import socket
from pathlib import Path

class SingleInstanceApp:
    def __init__(self, port_base=12345):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Create unique port for each user based on username
        username = os.getenv('USERNAME', 'default')
        self.port = port_base + hash(username) % 1000  # Different port for each user
        
        try:
            self.sock.bind(('localhost', self.port))
        except socket.error:
            messagebox.showinfo("Already Running", "Application is already running!")
            sys.exit()

class LauncherApp:
    def __init__(self, root):
        self.root = root
        self.root.title(f"4x4 Application Launcher - {os.getenv('USERNAME', 'User')}")
        
        # Prevent the window from being destroyed when clicking X
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Keep window on top and set window properties
        self.root.attributes('-topmost', True)
        self.root.resizable(False, False)
        
        # Set up user-specific config directory
        self.user_config_dir = os.path.join(
            os.path.expanduser('~'),
            'AppData',
            'Local',
            'LauncherApp'
        )
        os.makedirs(self.user_config_dir, exist_ok=True)
        
        # Load saved configurations from user directory
        self.config_file = os.path.join(self.user_config_dir, "launcher_config.json")
        self.button_configs = {}
        self.load_config()
        
        # Create main frame
        self.main_frame = ttk.Frame(root, padding="10")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Create 4x4 grid of buttons
        self.buttons = []
        for i in range(4):
            for j in range(4):
                btn = ttk.Button(
                    self.main_frame,
                    text=f"Button {i*4 + j + 1}",
                    width=15,
                    command=lambda x=i, y=j: self.launch_item(x, y)
                )
                btn.grid(row=i, column=j, padx=5, pady=5)
                self.buttons.append(btn)
                
                # Right-click menu for configuration
                btn.bind('<Button-3>', lambda e, x=i, y=j: self.show_config_menu(e, x, y))
        
        # Apply saved configurations
        self.update_buttons()
        
        # Save window position when moved
        self.root.bind('<Configure>', self.save_window_position)
        
        # Load and apply last window position
        self.load_window_position()
        
        # Add a status label showing config location
        status_text = f"Personal settings stored in: {self.user_config_dir}"
        status_label = ttk.Label(root, text=status_text, font=('Arial', 8), foreground='gray')
        status_label.grid(row=1, column=0, pady=(0, 5))
    
    def save_window_position(self, event=None):
        if event and event.widget == self.root:
            config = self.load_config_file()
            config['window'] = {
                'x': self.root.winfo_x(),
                'y': self.root.winfo_y()
            }
            self.save_config_file(config)
    
    def load_window_position(self):
        config = self.load_config_file()
        if 'window' in config:
            x = config['window'].get('x', 100)
            y = config['window'].get('y', 100)
            screen_width = self.root.winfo_screenwidth()
            screen_height = self.root.winfo_screenheight()
            
            x = min(max(0, x), screen_width - self.root.winfo_width())
            y = min(max(0, y), screen_height - self.root.winfo_height())
            
            self.root.geometry(f'+{x}+{y}')
    
    def load_config_file(self):
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        return {'buttons': {}, 'window': {}}
    
    def save_config_file(self, config):
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config, f)
        except Exception as e:
            print(f"Error saving config: {str(e)}")

    def on_closing(self):
        self.save_config()
        self.root.destroy()
        sys.exit()

    def show_config_menu(self, event, row, col):
        menu = tk.Menu(self.root, tearoff=0)
        menu.add_command(label="Select File or Folder", 
                        command=lambda: self.configure_button(row, col))
        menu.add_command(label="Set Label", 
                        command=lambda: self.set_button_label(row, col))
        menu.add_command(label="Clear", 
                        command=lambda: self.clear_button(row, col))
        menu.post(event.x_root, event.y_root)
    
    def configure_button(self, row, col):
        file_path = filedialog.askopenfilename(
            title="Select File",
            # Allow all file types
            filetypes=[("All Files", "*.*")],
            # Allow folder selection as well
            initialdir=os.path.expanduser("~")  # Start in user's home directory
        )
        
        # If user didn't select a file, try folder selection
        if not file_path:
            folder_path = filedialog.askdirectory(
                title="Select Folder",
                initialdir=os.path.expanduser("~")
            )
            if folder_path:
                path = folder_path
            else:
                return  # User cancelled both dialogs
        else:
            path = file_path

        if path:
            button_id = f"{row},{col}"
            # Get the base name, or use the last folder name for directory paths
            display_name = os.path.basename(path)
            if not display_name and os.path.isdir(path):  # For root directories
                display_name = path
            
            self.button_configs[button_id] = {
                "path": path,
                "label": display_name
            }
            self.update_buttons()
            self.save_config()
    
    def set_button_label(self, row, col):
        button_id = f"{row},{col}"
        if button_id in self.button_configs:
            dialog = tk.Toplevel(self.root)
            dialog.title("Set Button Label")
            
            label_var = tk.StringVar(value=self.button_configs[button_id]["label"])
            entry = ttk.Entry(dialog, textvariable=label_var)
            entry.pack(padx=10, pady=5)
            
            def save_label():
                self.button_configs[button_id]["label"] = label_var.get()
                self.update_buttons()
                self.save_config()
                dialog.destroy()
            
            ttk.Button(dialog, text="Save", command=save_label).pack(pady=5)
    
    def clear_button(self, row, col):
        button_id = f"{row},{col}"
        if button_id in self.button_configs:
            del self.button_configs[button_id]
            self.update_buttons()
            self.save_config()
    
    def launch_item(self, row, col):
        button_id = f"{row},{col}"
        if button_id in self.button_configs:
            path = self.button_configs[button_id]["path"]
            try:
                if os.path.isfile(path) or os.path.isdir(path):
                    os.startfile(path)
            except Exception as e:
                tk.messagebox.showerror("Error", f"Failed to open: {str(e)}")
    
    def update_buttons(self):
        for i in range(4):
            for j in range(4):
                button_id = f"{i},{j}"
                button = self.buttons[i*4 + j]
                if button_id in self.button_configs:
                    button.configure(text=self.button_configs[button_id]["label"])
                else:
                    button.configure(text=f"Button {i*4 + j + 1}")
    
    def save_config(self):
        config = self.load_config_file()
        config['buttons'] = self.button_configs
        self.save_config_file(config)
    
    def load_config(self):
        config = self.load_config_file()
        self.button_configs = config.get('buttons', {})

if __name__ == "__main__":
    # Ensure only one instance runs per user
    single_instance = SingleInstanceApp()
    
    root = tk.Tk()
    app = LauncherApp(root)
    root.mainloop()