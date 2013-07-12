/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.science.ml.client;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.cloudera.science.ml.client.cmd.Command;
import com.cloudera.science.ml.client.cmd.CommandException;
import com.cloudera.science.ml.client.cmd.GetHeaderCommand;
import com.cloudera.science.ml.client.cmd.KMeansAssignmentCommand;
import com.cloudera.science.ml.client.cmd.KMeansCommand;
import com.cloudera.science.ml.client.cmd.KMeansSketchCommand;
import com.cloudera.science.ml.client.cmd.LloydsCommand;
import com.cloudera.science.ml.client.cmd.NormalizeCommand;
import com.cloudera.science.ml.client.cmd.PivotCommand;
import com.cloudera.science.ml.client.cmd.SampleCommand;
import com.cloudera.science.ml.client.cmd.ShowVecCommand;
import com.cloudera.science.ml.client.cmd.SummaryCommand;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;

public class Main extends Configured implements Tool {
  
  private JCommander jc;
  private Help help = new Help();
  
  private static Set<String> HELP_ARGS = ImmutableSet.of("-h", "-help", "--help", "help");
  
  private static final Map<String, Command> COMMANDS = ImmutableSortedMap.<String, Command>naturalOrder()
      .put("header", new GetHeaderCommand())
      .put("lloyds", new LloydsCommand())
      .put("sample", new SampleCommand())
      .put("showvec", new ShowVecCommand())
      .put("summary", new SummaryCommand())
      .put("normalize", new NormalizeCommand())
      .put("kassign", new KMeansAssignmentCommand())
      .put("ksketch", new KMeansSketchCommand())
      .put("kmeans", new KMeansCommand())
      .put("pivot", new PivotCommand())
      .build();
  
  public Main() {
    jc = new JCommander(this);
    jc.setProgramName("ml");
    jc.addCommand("help", help, "-h", "-help", "--help");
    for (Map.Entry<String, Command> e : COMMANDS.entrySet()) {
      jc.addCommand(e.getKey(), e.getValue());
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      help.usage(jc, COMMANDS);
      return 1;
    }
    
    try {
      jc.parse(args);
    } catch (ParameterException pe) {
      String cmd = jc.getParsedCommand();
      if (COMMANDS.containsKey(cmd)) {
        boolean helped = false;
        if (args.length == 1) { // i.e., just the command
          jc.usage(cmd);
          helped = true;
        } else {
          for (String arg : args) {
            if (HELP_ARGS.contains(arg)) {
              jc.usage(cmd);
              helped = true;
              break;
            }
          }
        }
        if (!helped) {
          System.err.println(pe.getMessage());
        }
      } else {
        System.err.println(
            String.format("Did not recognize command '%s'. Type 'help' to get a list of all commands.",
                cmd));
      }
      return 1;
    }
    
    if ("help".equals(jc.getParsedCommand())) {
      return help.usage(jc, COMMANDS);
    }
    
    Command cmd = COMMANDS.get(jc.getParsedCommand());
    if (cmd == null) {
      return help.usage(jc, COMMANDS);
    }
    try {
      return cmd.execute(getConf());
    } catch (CommandException ce) {
      System.err.println("Command Error: " + ce.getMessage());
      return 1;      
    } catch (IllegalArgumentException e) {
      System.err.println("Argument Error: " + e.getMessage());
      return 1;
    } catch (IllegalStateException e) {
      System.err.println("State Error: " + e.getMessage());
      return 1;
    }
  }
  
  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new Main(), args);
    System.exit(rc);
  }
}
