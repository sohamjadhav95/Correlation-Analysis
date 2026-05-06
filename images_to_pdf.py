import os
try:
    from PIL import Image
    Image.init()  # Force load all image plugins to prevent KeyError: 'JPEG'
except ImportError:
    print("Pillow library is not installed. Please install it by running: pip install Pillow")
    import sys
    sys.exit(1)

def convert_images_to_pdf(directory_path, output_filename="combined_images.pdf"):
    """
    Finds all images in the specified directory and combines them into a single PDF.
    """
    if not os.path.isdir(directory_path):
        print(f"Error: The directory '{directory_path}' does not exist.")
        return

    # Supported image extensions
    valid_extensions = ('.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff')
    
    image_files = []
    for f in os.listdir(directory_path):
        if f.lower().endswith(valid_extensions):
            full_path = os.path.join(directory_path, f)
            image_files.append(full_path)
            
    if not image_files:
        print(f"No images found in '{directory_path}'.")
        return

    # Sort files by creation/modification time so they appear in the order they were taken
    image_files.sort(key=os.path.getmtime)

    print(f"Found {len(image_files)} images. Preparing PDF...")

    # Open the first image to use as the base for the PDF
    try:
        first_image = Image.open(image_files[0]).convert('RGB')
    except Exception as e:
        print(f"Error opening first image '{image_files[0]}': {e}")
        return
    
    # Open the rest of the images and append them to a list
    other_images = []
    for file_path in image_files[1:]:
        try:
            img = Image.open(file_path).convert('RGB')
            other_images.append(img)
        except Exception as e:
            print(f"Warning: Could not process image '{file_path}': {e}")

    # Set the output path to save inside the same directory
    output_path = os.path.join(directory_path, output_filename)

    print(f"Saving PDF to '{output_path}'...")
    
    # Save as PDF
    first_image.save(
        output_path,
        save_all=True,
        append_images=other_images,
        resolution=100.0
    )
    
    print("Done! PDF generated successfully.")

if __name__ == "__main__":
    # --- Configuration ---
    # Replace this with the directory containing your screenshots/images.
    # On Windows, standard screenshots via Win+PrtSc are usually saved at:
    # r"C:\Users\<YourUsername>\Pictures\Screenshots"
    TARGET_DIRECTORY = r"C:\Users\soham\OneDrive\Pictures\Screenshots"
    
    # Name of the output PDF file
    OUTPUT_PDF_NAME = "combined_screenshots.pdf"
    
    convert_images_to_pdf(TARGET_DIRECTORY, OUTPUT_PDF_NAME)
