import os
import sys
import winshell
from win32com.client import Dispatch

def create_shortcut():
    # Get the user's desktop directory
    desktop = winshell.desktop()

    # Define the path for the shortcut
    shortcut_path = os.path.join(desktop, "Reflexx Tracker.lnk")

    # Define the target exe file (assuming tracker_script.exe is in the same folder as create_shortcut.py)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    target = os.path.join(script_dir, "tracker_script.exe")

    # Define icon path (ensure R Icon.ico exists in the same directory)
    icon = os.path.join(script_dir, "R Icon.ico")

    # Create the shortcut
    shell = Dispatch('WScript.Shell')
    shortcut = shell.CreateShortcut(shortcut_path)
    shortcut.TargetPath = target
    shortcut.WorkingDirectory = script_dir
    shortcut.IconLocation = icon if os.path.exists(icon) else target
    shortcut.Save()

    print(f"Shortcut created at: {shortcut_path}")

if __name__ == "__main__":
    create_shortcut()
