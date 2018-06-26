package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	
	BufferedReader br;
	String fileName;
	Header header;
	/*
	 * parameterized constructor to initialize filename. As you are trying to
	 * perform file reading, hence you need to be ready to handle the IO Exceptions.
	 */
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		br = new BufferedReader(new FileReader(new File(fileName)));
	}

	/*
	 * implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 */
	@Override
	public Header getHeader() throws IOException {
		header = new Header();
		br = new BufferedReader(new FileReader(new File(fileName)));
		String contentLine = br.readLine();// read the first line
		header.headerArray = contentLine.split(",");
		for(String s: header.headerArray)
	    System.out.println(s);		
		// populate the header object with the String array containing the header names
		return header;
	}
	

	/**
	 * This method will be used in the upcoming assignments
	 */
	@Override
	public void getDataRow() {

	}

	/*
	 * implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. In
	 * the previous assignment, we have tried to convert a specific field value to
	 * Integer or Double. However, in this assignment, we are going to use Regular
	 * Expression to find the appropriate data type of a field. Integers: should
	 * contain only digits without decimal point Double: should contain digits as
	 * well as decimal point Date: Dates can be written in many formats in the CSV
	 * file. However, in this assignment,we will test for the following date
	 * formats('dd/mm/yyyy',
	 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm-dd')
	 */
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		DataTypeDefinitions dataTypeDefinations = new DataTypeDefinitions();		
		List<String> lines = new ArrayList<>();
		String line = null;
		
		//if file is not found we are setting filename to ipl.csv
		try {
			br = new BufferedReader(new FileReader(new File(fileName)));
		}
		catch(FileNotFoundException e) {
			br = new BufferedReader(new FileReader(new File("data/ipl.csv")));
		}
		while ((line = br.readLine()) != null) {
		    lines.add(line);
		}
		
		String[] linesArray = lines.toArray(new String[lines.size()]);
		int numberOfColumns = linesArray[0].split(",").length;
		dataTypeDefinations.row1 = linesArray[1].split(",",numberOfColumns);
		dataTypeDefinations.dataTypeOfColumns = new String[numberOfColumns];
		for(int i =0;i<dataTypeDefinations.row1.length;i++) {
			String fieldValue = dataTypeDefinations.row1[i];
			if(fieldValue.matches("[0-9]+")) {
				dataTypeDefinations.dataTypeOfColumns[i]= "java.lang.Integer";
			}
			else if(fieldValue.matches("[0-9]+.[0-9]+")) {
				dataTypeDefinations.dataTypeOfColumns[i]= "java.lang.Double";
			}
		    else if(fieldValue.matches("^[0-9]{2}/[0-9]{2}/[0-9]{4}$") | fieldValue.matches("^[0-9]{4}-[0-9]{2}-[0-9]{2}$") | fieldValue.matches("^[0-9]{2}-[a-z]{3}-[0-9]{2}$") | fieldValue.matches("^[0-9]{2}-[a-z]{3}-[0-9]{4}$") | fieldValue.matches("[0-9]{2}-[a-z]{3,9}-[0-9]{2}") | fieldValue.matches("^[0-9]{2}\\-[a-z]{3,9}\\-[0-9]{4}$")) {
		    	dataTypeDefinations.dataTypeOfColumns[i]= "java.util.Date";
		    }
		    else if(fieldValue.isEmpty()) {
		    	dataTypeDefinations.dataTypeOfColumns[i]= "java.lang.Object";
		    }
		    else {
		    	dataTypeDefinations.dataTypeOfColumns[i]= "java.lang.String";
		    }
		}
		return dataTypeDefinations;
	}
	}
	