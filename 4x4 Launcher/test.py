import tkinter as tk

root = tk.Tk()
root.title("Test Window")
label = tk.Label(root, text="If you see this, Python and tkinter are working!")
label.pack()
root.mainloop()