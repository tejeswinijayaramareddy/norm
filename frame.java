package norm;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.sql.SQLException;

import javax.swing.*;
import javax.swing.border.Border;

public class frame extends JFrame {
    public String PathSchema = "";
    public static void main(String[] args) {
        new frame().setVisible(true);
        //JFrame frame = new JFrame();
    }

    public frame(){
        super("Normazilation Project");
        setSize(400,300);
        setLocationRelativeTo(null);
        setResizable(false);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new FlowLayout());

        JLabel labelSelect  = new JLabel("", JLabel.LEFT);
        labelSelect.setText("Select your schema file for Normalization checking: ");

        JButton getFileButton = new JButton("Select File");

        JTextArea SchemaPathTxt = new JTextArea(2,30);
        SchemaPathTxt.setEnabled(false);
        Border border = BorderFactory.createLineBorder(Color.BLACK);
        SchemaPathTxt.setBorder(border);

        JButton ProcessButton = new JButton("Process");
        ProcessButton.setEnabled(false);

        JLabel FinishedP  = new JLabel("Process finished successfully.");
        FinishedP.setVisible(false);
        JLabel Output  = new JLabel("Output files generated on Desktop as NF.txt and NF.sql");
        Output.setVisible(false);


        add(labelSelect );
        add(getFileButton);
        add(SchemaPathTxt);
        add(ProcessButton);
        add(FinishedP);
        add(Output);
        getFileButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                JFileChooser fileChooser = new JFileChooser();
                int returnValue = fileChooser.showOpenDialog(null);
                if (returnValue == JFileChooser.APPROVE_OPTION) {
                    SchemaPathTxt.setText("");
                    File selectedFile = fileChooser.getSelectedFile();
                    SchemaPathTxt.setText
                            (selectedFile.getAbsolutePath());
                    PathSchema = selectedFile.getAbsolutePath();
                    FinishedP.setVisible(false);
                    Output.setVisible(false);
                    ProcessButton.setEnabled(true);
                }
            }
        });
        ProcessButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                postgres myObject = new postgres();
                try {
                    myObject.connection(PathSchema);
                    FinishedP.setVisible(true);
                    Output.setVisible(true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });

    }
}