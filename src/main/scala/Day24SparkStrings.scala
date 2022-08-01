
//import com.github.valrcs.SparkUtil.{getSpark, readCSVWithView}
import SparkUtil.{getSpark, readCSVWithView}
import org.apache.spark.sql.functions.{col, initcap, lit, lower, lpad, ltrim, regexp_replace, rpad, rtrim, trim, upper}

object Day24SparkStrings extends App {
  println("Chapter 6: Working with Strings")
  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = readCSVWithView(spark, filePath)

  //Working with Strings
  //String manipulation shows up in nearly every data flow, and it’s worth explaining what you can
  //do with strings. You might be manipulating log files performing regular expression extraction or
  //substitution, or checking for simple string existence, or making all strings uppercase or
  //lowercase.
  //Let’s begin with the last task because it’s the most straightforward. The initcap function will
  //capitalize every word in a given string when that word is separated from another by a space

  df.select(col("Description"),
    initcap(col("Description"))).show(3, false)

  //all SQL functions are listed here; https://spark.apache.org/docs/latest/api/sql/index.html
  spark.sql("SELECT Description, initcap(Description) FROM dfTable")
    .show(3, false) //false shows full strings in columns, without truncation/cutting

  //As just mentioned, you can cast strings in uppercase and lowercase, as well:

  df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))) //the lower here is not necessary of course example shows nesting
    .show(3, false)

  spark.sql("SELECT Description, lower(Description), " +
    "upper(lower(Description)) FROM dfTable") //again upper(lower is not needed but it might be useful for some other function
    .show(3, false)

  //Another trivial task is adding or removing spaces around a string. You can do this by using lpad,
  //ltrim, rpad and rtrim, trim:

  df.select(
    col("CustomerId"), //not needed just to show you we are working with the dateframe
    ltrim(lit(" HELLO ")).as("ltrim"),
    rtrim(lit(" HELLO ")).as("rtrim"),
    trim(lit(" HELLO ")).as("trim"),
    lpad(lit("HELLO"), 3, " ").as("lp"),
    rpad(lit("HELLO"), 10, " ").as("rp"),
    //😁 is represented by 4 bytes thats why you have \u twice
    lpad(rpad(lit("HELLO"), 10, "*"), 15, "\uD83D\uDE01").as("pad15charstotal")
  ).show(2)
  //so pad even works with high value unicode after 128k which is smileys

  //so lpad (rpad is similar)
  //Left-pad the string column with pad to a length of len.
  // If the string column is longer than len, the return value is shortened to len characters.

  spark.sql(
    """
      |SELECT
      | CustomerId,
      |ltrim(' HELLLOOOO ') as ltrim,
      |rtrim(' HELLLOOOO '),
      |trim(' HELLLOOOO '),
      |lpad('HELLOOOO ', 3, ' '),
      |rpad('HELLOOOO ', 10, ' ')
      | FROM dfTable
      |""".stripMargin)
    .show(2)


  //Regular Expressions
  //Probably one of the most frequently performed tasks is searching for the existence of one string
  //in another or replacing all mentions of a string with another value. This is often done with a tool
  //called regular expressions that exists in many programming languages. Regular expressions give
  //the user an ability to specify a set of rules to use to either extract values from a string or replace
  //them with some other values.
  //Spark takes advantage of the complete power of Java regular expressions. The Java regular
  //expression syntax departs slightly from other programming languages, so it is worth reviewing
  //before putting anything into production. There are two key functions in Spark that you’ll need in
  //order to perform regular expression tasks: regexp_extract and regexp_replace. These
  //functions extract values and replace values, respectively.
  //Let’s explore how to use the regexp_replace function to replace substitute color names in our
  //description column:

  //prepping regex in Scala - could do it by hand of course by writing up full regex
  val simpleColors = Seq("black", "white", "red", "green", "blue", "cream", "metal", "wood")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|")
  println(regexString) //"BLACK|WHITE|RED|GREEN|BLUE"

  df.select(
    regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
    col("Description"))
    .show(5, false)

  spark.sql(
    """
      |SELECT
      |regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'colorful') as
      |color_clean, Description
      |FROM dfTable
      |""".stripMargin)
    .show(5, false)

  //TODO open up March 1st, of 2011 CSV
  val filePath2 = "src/resources/retail-data/by-day/2011-03-01.csv"

  val dfMarch = readCSVWithView(spark, filePath)
  //Select Capitalized Description Column
  dfMarch.select(col("Description"),
    initcap(col("Description"))).show(3, false)
  //TODO Select Padded country column with _ on both sides with 30 characters for country name total allowed
  dfMarch.select(
    col("Country"),
    lpad(rpad(col("Country"), 17, "_"), 20, "_").as("padded Country")
  ).show(3, false) //does not work witk 30 total. Maybe to the field we can put limites up to 20 char total?

  //ideally there would be even number of _______LATVIA__________ (30 total)

  //select Description column again with all occurences of metal or wood replaced with material
  val simpleColors2 = Seq("metal", "wood")
  val regexString2 = simpleColors2.map(_.toUpperCase).mkString("|")
  dfMarch.select(
    regexp_replace(col("Description"), regexString2, "material").alias("material"),
    col("Description"))
    .show(10, false)

  //so this description white metal lantern -> white material lantern
  //then show top 10 results of these 3 columns



}