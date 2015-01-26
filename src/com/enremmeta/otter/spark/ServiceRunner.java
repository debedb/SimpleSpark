package com.enremmeta.otter.spark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * Implements some common services for classes that are intended to have
 * <tt>main()</tt>, such as command-line parsing, etc.
 * 
 * @author Gregory Golberg (grisha@alum.mit.edu)
 * 
 *         Copyright © <a href="http://www.enremmeta.com">Enremmeta LLC</a>
 *         2014. All Rights Reserved. This code is licensed under <a
 *         href="http://www.gnu.org/licenses/agpl-3.0.html">Affero GPL 3.0</a>
 *
 */
public abstract class ServiceRunner {

    protected Options opts;
    protected CommandLine cl;

    public ServiceRunner() {
	super();
	opts = new Options();
    }

    public CommandLine getCl() {
	return cl;
    }

    public Options getOpts() {
	return opts;
    }

    protected void parseCommandLineArgs(String[] argv) throws Exception {
	CommandLineParser parser = new PosixParser();
	try {
	    cl = parser.parse(opts, argv);
	} catch (ParseException e) {
	    usage();
	}
    }

    public void setCl(CommandLine cl) {
	this.cl = cl;
    }

    public void setOpts(Options opts) {
	this.opts = opts;
    }

    protected void usage() {
	HelpFormatter formatter = new HelpFormatter();
	formatter.printHelp("ant", opts);
	System.exit(1);
    }

}
