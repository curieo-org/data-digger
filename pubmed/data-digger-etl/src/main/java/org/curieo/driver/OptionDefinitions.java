package org.curieo.driver;

import java.util.Optional;
import lombok.Generated;
import lombok.Value;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.curieo.utils.ParseParameters;
import org.curieo.utils.ParseParametersString;

public class OptionDefinitions {
  static final String MESSAGE =
      "--link-table option must be of the format TABLENAME:IDTYPE1=IDTYPE2";
  static Option postgresUser =
      Option.builder()
          .option("p")
          .longOpt("postgres-user")
          .hasArg()
          .desc("postgresql user")
          .build();

  static Option batchSizeOption =
      Option.builder()
          .option("b")
          .longOpt("batch-size")
          .hasArg()
          .desc("the size of batches to be submitted to the SQL database")
          .type(Integer.class)
          .build();

  static Option useKeysOption =
      Option.builder().option("k").longOpt("use-keys").required(false).build();

  static Option awsStorageOption =
      Option.builder().option("a").longOpt("use-aws").required(false).build();

  static Option maxFiles =
      Option.builder()
          .option("m")
          .longOpt("maximum-files")
          .hasArg()
          .desc("maximum number of records to process")
          .type(Integer.class)
          .build();

  static Option firstYearOption =
      Option.builder()
          .option("f")
          .longOpt("first-year")
          .hasArg()
          .desc("first year (inclusive)")
          .type(Integer.class)
          .build();

  static Option lastYearOption =
      Option.builder()
          .option("l")
          .longOpt("last-year")
          .hasArg()
          .desc("last year (inclusive)")
          .type(Integer.class)
          .build();

  static Option references =
      Option.builder()
          .option("r")
          .longOpt("references")
          .desc("references to sql database (specify types, e.g. \"pubmed\")")
          .required(false)
          .hasArgs()
          .build();

  static final Option linkTable =
      Option.builder()
          .option("l")
          .longOpt("link-table")
          .desc("create a link table for links between x=y; " + MESSAGE)
          .required(false)
          .hasArgs()
          .build();

  static Optional<Integer> getIntOption(CommandLine cmd, Option option) {
    if (!cmd.hasOption(option)) return Optional.empty();
    try {
      return Optional.of(Integer.parseInt(cmd.getOptionValue(option)));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  @Generated
  @Value
  static class LinkTableOption {
    String table;
    String source;
    String target;

    static LinkTableOption parse(String option) {
      ParseParameters pp = new ParseParametersString(option);
      String table = pp.parseToken(Character::isLetterOrDigit);
      pp.expect(':', MESSAGE);
      pp.eat();
      String source = pp.parseToken(Character::isLetterOrDigit);
      pp.expect('=', MESSAGE);
      pp.eat();
      return new LinkTableOption(table, source, pp.parseToken(Character::isLetterOrDigit));
    }
  }
}
