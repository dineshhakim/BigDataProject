package ui;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import java.awt.BorderLayout;
import java.io.*;
import java.util.HashMap;
import java.util.Vector;

import main.MainClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class SwingApplicationWindow {
	private JFrame frame1;
	String[] args1 = new String[3];

	public static void main(String[] args) {
		try {
			SwingApplicationWindow window = new SwingApplicationWindow();
			window.frame1.setVisible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public SwingApplicationWindow() {
		initialize();
	}

	javax.swing.JPanel JPanel1 = new javax.swing.JPanel();

	javax.swing.JButton JButtonExecute = new javax.swing.JButton();
	javax.swing.JTextField JInputText = new javax.swing.JTextField();
	javax.swing.JTextField JOutputText = new javax.swing.JTextField();
	javax.swing.JLabel JInputLabel = new javax.swing.JLabel();
	javax.swing.JLabel JOutputLabel = new javax.swing.JLabel();

	JComboBox<String> JcomboBox = new JComboBox<String>();
	javax.swing.JList<String> JList1 = new javax.swing.JList<String>();
	javax.swing.JTable JTable1 = new javax.swing.JTable();
	private JScrollPane JScrollPane1;
	JComboBox<String> JcomboBoxCommand = new JComboBox<String>();
	javax.swing.JPanel jPanel = new javax.swing.JPanel();

	private void initialize() {
		JcomboBox.addItem("Cloudera");
		JcomboBox.addItem("HortonWorks");
		JcomboBoxCommand.addItem("pair");
		JcomboBoxCommand.addItem("stripe");
		JcomboBoxCommand.addItem("hybrid");
		frame1 = new JFrame();
		frame1.setTitle("Hdfs Connector");
		frame1.setDefaultCloseOperation(javax.swing.JFrame.DO_NOTHING_ON_CLOSE);
		frame1.getContentPane().setLayout(null);
		frame1.setSize(600, 600);
		frame1.setVisible(false);

		int x = 0;
		int y = 30;
		JcomboBox.setVisible(true);
		frame1.getContentPane().add(JcomboBox);
		JcomboBox.setBounds(78, x += y, 160, 30);

		JcomboBoxCommand.setVisible(true);
		frame1.getContentPane().add(JcomboBoxCommand);
		JcomboBoxCommand.setBounds(78, x += y + 10, 160, 30);

		JInputLabel.setVisible(true);
		JInputLabel.setText("Input");
		frame1.getContentPane().add(JInputLabel);
		JInputLabel.setBounds(78, x += y, 160, 30);

		JInputText.setVisible(true);
		JInputText.setText("inputdir");
		frame1.getContentPane().add(JInputText);
		JInputText.setBounds(78, x += y, 160, 30);

		JOutputLabel.setVisible(true);
		JOutputLabel.setText("Output");
		frame1.getContentPane().add(JOutputLabel);
		JOutputLabel.setBounds(78, x += y, 160, 30);

		JOutputText.setVisible(true);
		JOutputText.setText("output");
		frame1.getContentPane().add(JOutputText);
		JOutputText.setBounds(78, x += y, 160, 30);

		JButtonExecute.setText("Execute");
		frame1.getContentPane().add(JButtonExecute);
		JButtonExecute.setBounds(78, x += y, 160, 30);

		JPanel1.setLayout(null);
		frame1.getContentPane().add(BorderLayout.CENTER, JPanel1);
		JPanel1.setBounds(250, 30, 200, 500);
		JScrollPane1 = new JScrollPane();
		JPanel1.add(JScrollPane1);
		JScrollPane1.setBounds(250, 30, 200, 400);
		JScrollPane1.getViewport().add(JTable1);
		JTable1 = new JTable();
		frame1.getContentPane().add(JTable1);
		JTable1.setBounds(250, 30, 200, 500);

		SymWindow aSymWindow = new SymWindow();
		frame1.addWindowListener(aSymWindow);
		SymAction lSymAction = new SymAction();
		JButtonExecute.addActionListener(lSymAction);

	}

	class SymAction implements java.awt.event.ActionListener {
		public void actionPerformed(java.awt.event.ActionEvent event) {
			Object object = event.getSource();
			if (object == JButtonExecute)
				JButtonExecute_actionPerformed(event);

		}

	}

	class SymWindow extends java.awt.event.WindowAdapter {
		public void windowClosing(java.awt.event.WindowEvent event) {

			Object object = event.getSource();
			if (object == frame1) {
				windowClosing1(event);
			}
		}
	}

	void JButtonExecute_actionPerformed(java.awt.event.ActionEvent event) {
		GetHadoopPath(JcomboBoxCommand.getSelectedItem().toString());
		try {
			MainClass.main(args1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		GetFileFromHdfs();
	}

	void windowClosing1(java.awt.event.WindowEvent event) {
		// Object object = event.getSource();
		exitApplication();
	}

	void exitApplication() {
		try {
			frame1.setVisible(false); // hide the Frame
			frame1.dispose(); // free the system resources
			System.exit(0); // close the application
		} catch (Exception e) {
		}
	}

	void GetHadoopPath(String command) {
		args1[0] = JInputText.getText();
		args1[1] = JOutputText.getText();
		args1[2] = command;

		System.out.println(args1[0] + "     " + args1[1] + "    " + args1[2]);
		FileSystem hdfs;
		try {
			Configuration conf = new Configuration();
			hdfs = FileSystem.get(conf);
			// Print the home directory
			System.out.println("Home folder -" + hdfs.getHomeDirectory());
			// Create & Delete Directories
			Path workingDir = hdfs.getWorkingDirectory();
			Path newFolderPath = new Path(workingDir + "/" + args1[1]);
			System.out.println("Output Folder -" + newFolderPath);
			if (hdfs.exists(newFolderPath)) {
				// Delete existing Directory
				hdfs.delete(newFolderPath, true);
				System.out.println("Existing Folder Deleted.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private Object rowdata[];

	void GetFileFromHdfs() {
		FileSystem hdfs;
		try {
			Configuration conf = new Configuration();
			hdfs = FileSystem.get(conf);

			// Copying File from local to HDFS
			Path workingDir = hdfs.getWorkingDirectory();
			Path newFolderPath = new Path(workingDir + "/" + args1[1]
					+ "/part-r-00000");
			System.out.println("Reading from HDFS file.");

			BufferedReader bfr = new BufferedReader(new InputStreamReader(
					hdfs.open(newFolderPath)));
			String str = null;
			DefaultTableModel model = new DefaultTableModel();
			model.addColumn("Key");
			model.addColumn("Value");
			rowdata = new Object[2];
			while ((str = bfr.readLine()) != null) {
				String[] allIds = str.split("\\s");
				System.out.println(str);
				 
				if (allIds.length < 2) {
					rowdata[0] = allIds[0];

					rowdata[1] = "";
				} else {
					rowdata[0] = allIds[0];
					rowdata[1] = allIds[1];					 
				}

				model.addRow(rowdata);

			}

			JTable1.setModel(model);
			 

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
