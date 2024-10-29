from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from typing import Dict, Any
from pathlib import Path


def wrap_text(text, max_width, canvas, x_position, y_position, line_height):
    """
    Wrap text to fit within a specified width.

    Parameters:
    - text: str, the text to wrap.
    - max_width: int, maximum width in points.
    - canvas: canvas, the PDF canvas to draw on.
    - x_position: int, starting x-coordinate.
    - y_position: int, starting y-coordinate.
    - line_height: int, line spacing.

    Returns:
    - y_position after the last line drawn.
    """
    words = text.split()
    line = ""
    for word in words:
        test_line = f"{line} {word}".strip()
        if canvas.stringWidth(test_line) > max_width:
            canvas.drawString(x_position, y_position, line)
            y_position -= line_height
            line = word
        else:
            line = test_line
    if line:  # Draw any remaining text
        canvas.drawString(x_position, y_position, line)
        y_position -= line_height
    return y_position


def generate_processing_report(file_name: str, processing_data: Dict[str, Any], save_path: str = "output") -> str:
    """
    Generate a PDF report for the processing steps, parameters, and results.

    Parameters:
    - file_name: str
        The name of the file processed.
    - processing_data: Dict[str, Any]
        Dictionary containing details from processing, including parameters, steps, and results.
    - save_path: str
        Directory where the PDF will be saved.

    Returns:
    - str: The path to the generated PDF file.
    """
    Path(save_path).mkdir(parents=True, exist_ok=True)
    pdf_path = Path(save_path) / f"{file_name}_report.pdf"

    # Initialize the PDF canvas
    c = canvas.Canvas(str(pdf_path), pagesize=A4)
    width, height = A4
    y_position = height - 50
    max_text_width = width - 100  # Max width for text with padding

    # Title and file name
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y_position, f"Processing Report for File: {file_name}")
    y_position -= 30
    c.setFont("Helvetica", 10)
    c.drawString(50, y_position, f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    y_position -= 40

    # Metadata Section
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y_position, "Metadata:")
    y_position -= 20
    c.setFont("Helvetica", 10)
    metadata = processing_data.get("metadata", {})
    for key, value in metadata.items():
        c.drawString(50, y_position, f"{key}: {value}")
        y_position -= 15
    y_position -= 10

    # Processing Steps
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y_position, "Processing Steps:")
    y_position -= 20
    c.setFont("Helvetica", 10)
    processing_steps = [
        "Data Conversion (raw to Zarr)",
        "Sv Computation",
        "Metadata Extraction",
        "Sending to IoT Hub",
        "Echogram Generation and Upload"
    ]
    for step in processing_steps:
        c.drawString(50, y_position, f"- {step}")
        y_position -= 15
    y_position -= 10

    # Parameters Section
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y_position, "Parameters Used:")
    y_position -= 20
    c.setFont("Helvetica", 10)
    parameters = processing_data.get("parameters", {})
    for key, value in parameters.items():
        c.drawString(50, y_position, f"{key}: {value}")
        y_position -= 15
    y_position -= 10

    # Results Section
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y_position, "Results:")
    y_position -= 20
    c.setFont("Helvetica", 10)
    results = processing_data.get("results", {})
    for key, value in results.items():
        c.drawString(50, y_position, f"{key}: {value}")
        y_position -= 15
    y_position -= 10

    # Geospatial Data with Wrapped Text
    gps_data = processing_data.get("gps_data", [])
    if gps_data:
        c.setFont("Helvetica-Bold", 12)
        c.drawString(50, y_position, "Geospatial Data:")
        y_position -= 20
        c.setFont("Helvetica", 10)

        for point in gps_data:
            lat = point.get("lat")
            lon = point.get("lon")
            timestamp = point.get("dt", "N/A")
            speed_knots = point.get("knt", "N/A")

            if lat is not None and lon is not None:
                location_str = (
                    f"Latitude: {lat}, Longitude: {lon}, "
                    f"Timestamp: {timestamp}, Speed (knots): {speed_knots}"
                )
            else:
                location_str = f"Location data not available, Timestamp: {timestamp}"

            # Wrap text and adjust y_position as needed
            y_position = wrap_text(location_str, max_text_width, c, 50, y_position, 15)
            if y_position < 40:  # Start new page if needed
                c.showPage()
                y_position = height - 50
                c.setFont("Helvetica", 10)

        y_position -= 10

    # Processing Time Summary
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y_position, "Processing Summary:")
    y_position -= 20
    c.setFont("Helvetica", 10)
    c.drawString(50, y_position, f"Total Processing Time (ms): {processing_data.get('processing_time_ms')}")
    y_position -= 15
    c.drawString(50, y_position,
                 f"Echogram Processing Time (ms): {processing_data.get('processing_time_ms_echograms', 'N/A')}")
    y_position -= 15

    # End the page and save
    c.showPage()
    c.save()

    return str(pdf_path)
