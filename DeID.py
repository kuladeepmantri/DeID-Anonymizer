from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, sha2
from pyspark.sql.types import DoubleType, StringType
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QFileDialog, QVBoxLayout, QWidget, QLabel, QLineEdit,
                             QPushButton, QComboBox, QSpinBox, QFormLayout, QGroupBox, QHBoxLayout, QMessageBox,
                             QCheckBox, QAction, QStyleFactory, QProgressBar, QSizePolicy,QProgressDialog )
from PyQt5.QtCore import Qt, QSize, QTimer, QThread
from PyQt5.QtGui import QIcon, QColor

import sys
    
class LoadDatasetThread(QThread):
    def __init__(self, file_path):
        QThread.__init__(self)
        self.file_path = file_path

    def run(self):
        self.df = load_dataset(self.file_path)
      

def load_dataset(file_path):
    spark = SparkSession.builder.master("local").appName("K-Anonymity").getOrCreate()

    file_type = file_path.split(".")[-1].lower()
    if file_type == "csv":
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    elif file_type == "json":
        df = spark.read.json(file_path, multiLine=True)
    else:
        raise ValueError("Unsupported file type")
    return df

def save_dataset(df, file_path):
    file_type = file_path.split(".")[-1].lower()
    if file_type == "csv":
        df.write.csv(file_path, mode="overwrite", header=True)
    elif file_type == "json":
        df.write.json(file_path, mode="overwrite")
    else:
        raise ValueError("Unsupported file type")

def generalize_column(df, col_name, interval_size):
    if df.schema[col_name].dataType == StringType():
        # For string columns, anonymize by replacing with a hash of the original value
        return df.withColumn(col_name, sha2(col(col_name), 256))
    else:
        # For numeric columns, perform generalization
        return df.withColumn(col_name, (floor(col(col_name).cast(DoubleType()) / interval_size) * interval_size).cast(df.schema[col_name].dataType))

def k_anonymize(df, k, generalization_columns):
    for col_name, interval_size in generalization_columns.items():
        if col_name in df.columns:
            df = df.transform(lambda df: generalize_column(df, col_name, interval_size))
    return df

class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super(MainWindow, self).__init__(parent)
        self.setWindowTitle("DeID")
        self.setMinimumSize(600, 400)
        
        self.df = None
        

        self.load_file_icon_light = QIcon("icons/load_file2.png")
        self.load_file_icon_dark = QIcon("icons/dark.png")

        self.dark_mode = False
        
        # Create table widget
        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setRowCount(2)

        # Set table properties
        self.table.setHorizontalHeaderLabels(['Column 1', 'Column 2', 'Column 3'])
        self.table.setItem(0, 0, QTableWidgetItem('1'))
        self.table.setItem(0, 1, QTableWidgetItem('abc'))
        self.table.setItem(0, 2, QTableWidgetItem('xyz'))
        self.table.setItem(1, 0, QTableWidgetItem('2'))
        self.table.setItem(1, 1, QTableWidgetItem('def'))
        self.table.setItem(1, 2, QTableWidgetItem('uvw'))

        # Add table to main window
        self.setCentralWidget(self.table)

        # Show hint message
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle("Hint")
        msg.setText("Remember: numbers are safe, but strings are sneaky. We'll use k-anonymity to keep your digits private, but your letters will be turned into anonymous hashes. Choose your columns wisely!")
        msg.exec_()
        

        self.init_ui()

    def init_ui(self):
        # Main layout
        main_layout = QVBoxLayout()

        # #Add night mode toggle
        # night_mode_action = QAction(QIcon(), "Dark Mode", self)
        # night_mode_action.triggered.connect(self.toggle_night_mode)
        # self.toolbar = self.addToolBar("Night Mode")
        # self.toolbar.addAction(night_mode_action)
        
        
                # Add night mode toggle
        self.night_mode_action = QAction(QIcon(), "Activate Stealth Mode", self)
        self.night_mode_action.triggered.connect(self.toggle_night_mode)
        self.toolbar = self.addToolBar("Night Mode")
        self.toolbar.addAction(self.night_mode_action)

        # Load file
        load_file_layout = QHBoxLayout()
        self.file_path_edit = QLineEdit()
        self.load_file_button = QPushButton()  # Create the load_file_button
        self.load_file_button.clicked.connect(self.load_file)
        load_file_layout.addWidget(QLabel("File Path:"))
        load_file_layout.addWidget(self.file_path_edit)
        load_file_layout.addWidget(self.load_file_button)
        self.load_file_button.setIcon(self.load_file_icon_light)


        # Column generalization settings
        self.generalization_group = QGroupBox("Column Generalization Settings")
        self.generalization_group.setEnabled(False)
        self.generalization_form = QFormLayout()

        # K-anonymity settings
        k_anonymity_layout = QHBoxLayout()
        self.k_spinbox = QSpinBox()
        self.k_spinbox.setRange(1, 100)
        k_anonymity_layout.addWidget(QLabel("K Value:"))
        k_anonymity_layout.addWidget(self.k_spinbox)

        # Save file
        save_file_layout = QHBoxLayout()
        self.output_path_edit = QLineEdit()
        save_file_button = QPushButton("Save File")
        save_file_button.clicked.connect(self.save_file)
        save_file_layout.addWidget(QLabel("Output File Path:"))
        save_file_layout.addWidget(self.output_path_edit)
        save_file_layout.addWidget(save_file_button)

        # Run button
        run_button = QPushButton("Run Anonymization")
        run_button.clicked.connect(self.run_anonymization)

        # Add widgets to the main layout
        main_layout.addLayout(load_file_layout)
        main_layout.addWidget(self.generalization_group)
        main_layout.addLayout(k_anonymity_layout)
        main_layout.addLayout(save_file_layout)
        main_layout.addWidget(run_button)

        
        
                # CHANGES: Add progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)

        # CHANGES: Add size policies to ensure responsive design
        self.file_path_edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.output_path_edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.generalization_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.MinimumExpanding)

        # Set the main layout
        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)


            
    def load_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open File", "", "CSV Files (*.csv);;JSON Files (*.json);;All Files (*)")
        if file_path:
            self.file_path_edit.setText(file_path)

            # Create and show the progress dialog
            progress_dialog = QProgressDialog("Loading data...", "Cancel", 0, 100, self)
            progress_dialog.setWindowTitle("Please wait")
            progress_dialog.setWindowModality(Qt.WindowModal)
            progress_dialog.setAutoClose(False)
            progress_dialog.setAutoReset(False)
            progress_dialog.setMinimumDuration(0)
            progress_dialog.setValue(0)
            progress_dialog.show()

            # Create the load dataset thread
            load_thread = LoadDatasetThread(file_path)
            load_thread.finished.connect(lambda: self.handle_thread_finished(load_thread, progress_dialog))
            load_thread.start()

            # Create a timer to update the progress dialog
            timer = QTimer()
            timer.timeout.connect(lambda: self.update_progress_dialog(progress_dialog, load_thread))
            timer.start(100)
            
            
    def handle_thread_finished(self, load_thread, progress_dialog):
            progress_dialog.close()
            self.df = load_thread.df
            self.generalization_group.setEnabled(True)
            self.init_generalization_form()




    def update_progress_dialog(self, progress_dialog, load_thread):
        # Update progress dialog
        value = progress_dialog.value() + 1
        progress_dialog.setValue(value % 100)

        # Check if the thread has finished loading the dataset
        if not load_thread.isRunning():
            progress_dialog.close()
            self.df = load_thread.df
            self.generalization_group.setEnabled(True)
            self.init_generalization_form()


    def init_generalization_form(self):
        # Remove all rows from the existing form layout
        while self.generalization_form.rowCount() > 0:
            for i in range(self.generalization_form.count()):
                item = self.generalization_form.itemAt(i)
                if item is not None:
                    widget = item.widget()
                    if widget is not None:
                        widget.deleteLater()
            self.generalization_form.removeRow(0)

        # Create the form layout for the columns
        self.column_spinboxes = {}
        self.column_checkboxes = {}
        for col_name in self.df.columns:
            spinbox = QSpinBox()
            spinbox.setRange(1, 100)
            spinbox.setEnabled(False)  # Disable the spinbox initially
            
            checkbox = QCheckBox()
            checkbox.setStyleSheet("QCheckBox::indicator { width: 16px; height: 16px; } QCheckBox::indicator:checked { background-color: green; }")
            checkbox.stateChanged.connect(lambda state, s=spinbox: s.setEnabled(state == Qt.Checked))  # Enable or disable spinbox based on the checkbox state
            
            row_layout = QHBoxLayout()
            row_layout.addWidget(spinbox)
            row_layout.addWidget(checkbox)
            
            self.generalization_form.addRow(f"{col_name} Interval Size:", row_layout)
            self.column_spinboxes[col_name] = spinbox
            self.column_checkboxes[col_name] = checkbox

        self.generalization_group.setLayout(self.generalization_form)






    def save_file(self):
        output_path, _ = QFileDialog.getSaveFileName(self, "Save File", "", "CSV Files (*.csv);;JSON Files (*.json);;All Files (*)")
        if output_path:
            self.output_path_edit.setText(output_path)

    def run_anonymization(self):
            if not self.file_path_edit.text() or not self.output_path_edit.text():
                QMessageBox.warning(self, "Error", "Please provide both input file and output file paths.")
                return
            
            
                
                # CHANGES: Show progress bar while running the anonymization
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(25)

            # Update generalization_columns based on checked checkboxes
            generalization_columns = {col: spinbox.value() for col, spinbox in self.column_spinboxes.items()
                                    if self.column_checkboxes[col].isChecked()}
            k = self.k_spinbox.value()

            try:
                output_df = k_anonymize(self.df, k, generalization_columns)
                
                                # CHANGES: Update progress bar
                self.progress_bar.setValue(50)
                save_dataset(output_df, self.output_path_edit.text())
                QMessageBox.information(self, "Success", "Anonymization completed successfully.")
                        # CHANGES: Hide progress bar after successful anonymization
                self.progress_bar.setValue(100)
                self.progress_bar.setVisible(False)
            except Exception as e:
                QMessageBox.warning(self, "Error", f"An unexpected error occurred: {e}")
                
                                # CHANGES: Hide progress bar after an error
                self.progress_bar.setValue(0)
                self.progress_bar.setVisible(False)
                
    def set_colorful_mode(self, colorful_mode):
        if colorful_mode:
            QApplication.setPalette(self.create_colorful_palette())
            self.setStyleSheet("QToolTip { color: #00ff00; background-color: #202020; border: 1px solid #00ff00; } QSpinBox:disabled { color: grey; }")
        else:
            QApplication.setPalette(QApplication.style().standardPalette())
                
                        
    def toggle_night_mode(self):
        self.dark_mode = not self.dark_mode
        self.set_dark_mode(self.dark_mode)
        if self.dark_mode:
            self.load_file_button.setIcon(self.load_file_icon_dark)
            self.night_mode_action.setText("Deactivate Stealth Mode")
        else:
            self.load_file_button.setIcon(self.load_file_icon_light)
            self.night_mode_action.setText("Activate Stealth Mode")



        
    def set_dark_mode(self, dark_mode):
        if dark_mode:
            QApplication.setStyle(QStyleFactory.create('Fusion'))
            self.dark_palette = self.create_dark_palette()
            self.set_colorful_mode(False)

            QApplication.setPalette(self.dark_palette)
            self.setStyleSheet("QToolTip { color: #ffffff; background-color: #2a82da; border: 1px solid white; } QSpinBox:disabled { color: grey; }")
            #Set dark mode icon
            self.load_file_button.setIcon(self.load_file_icon_dark)
        else:
            QApplication.setStyle(QStyleFactory.create('Fusion'))
            QApplication.setPalette(QApplication.style().standardPalette())
            self.setStyleSheet("QToolTip { color: #000000; background-color: #f5f5f5; border: 1px solid #f0f0f0; } QSpinBox:disabled { color: grey; }")
            self.load_file_button.setIcon(self.load_file_icon_light)
            

        
    def create_dark_palette(self):
        dark_palette = QApplication.style().standardPalette()
        dark_palette.setColor(dark_palette.Window, QColor(53, 53, 53))
        dark_palette.setColor(dark_palette.WindowText, Qt.white)
        dark_palette.setColor(dark_palette.Base, QColor(25, 25, 25))
        dark_palette.setColor(dark_palette.AlternateBase, QColor(53, 53, 53))
        dark_palette.setColor(dark_palette.ToolTipBase, QColor(25, 25, 25))
        dark_palette.setColor(dark_palette.ToolTipText, Qt.white)
        dark_palette.setColor(dark_palette.Text, Qt.white)
        dark_palette.setColor(dark_palette.Button, QColor(53, 53, 53))
        dark_palette.setColor(dark_palette.ButtonText, Qt.white)
        dark_palette.setColor(dark_palette.BrightText, Qt.red)
        dark_palette.setColor(dark_palette.Link, QColor(42, 130, 218))
        dark_palette.setColor(dark_palette.Highlight, QColor(42, 130, 218))
        dark_palette.setColor(dark_palette.HighlightedText, Qt.black)
        return dark_palette
    
 
# Replace the existing main() function with this one
def main():
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.show()
    sys.exit(app.exec_())

# Add this line at the end of your script to call the main() function
if __name__ == "__main__":
    main()