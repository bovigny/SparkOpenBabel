/**
 * Created by christophebovigny on 31.07.15.
 * Small programm to compute molecule features
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.openbabel._

object MySimpleApp {

  // Define a test method to give the number of Atoms in the molecule : Return an Long format
  def getNumAtoms(File: String): Long ={
    // Create an object to convert molecule format like smiles to SDF etc...
    val c = new OBConversion
    // Create an object mol to define the molecule
    val mol = new OBMol
    // Set the input format of the molecule
    c.SetInFormat("smi")
    // Read the inputfile containing smiles molecule format
    c.ReadString(mol, File)
    // Get the number of Atoms for each molecules
    val numAtoms = mol.NumAtoms()
    // Return the number of atoms
    return numAtoms

  }



  // Computation of FP2 Fingerprint  : Return a vector of Unsigned Int
  // FP2 : http://openbabel.org/wiki/FP2
  def computeFP2(File:String): vectorUnsignedInt = {
    // Create an object to convert molecule format like smiles to SDF etc...
    val c = new OBConversion
    // Create an object mol to define the molecule
    val mol = new OBMol
    // Set the input format of the molecule
    c.SetInFormat("smi")
    //c.SetInAndOutFormats("smi", "fpt")
    // Read the inputfile containing smiles molecule format
    c.ReadString(mol, File)
    // Define the fingerprint that we want : FP2, FP3, FP4
    val fingerprinter = OBFingerprint.FindFingerprint("FP2")
    // Define the vector that will contain the fingerprint
    val vfp = new vectorUnsignedInt()
    // Compute the fingerprint for each molecule in the input file and return the vfp which is a vectorUnsignedInt
    fingerprinter.GetFingerprint(mol,vfp)
    // Return the vector
    return vfp
  }

//def compaireFing(molecule:String): Double = {
  def compareFingerPrint(File: String, molecule:String): Double = {
    val c = new OBConversion
    val mol = new OBMol
    c.SetInFormat("smi")
    c.ReadString(mol, File)

    val fingerprinter = OBFingerprint.FindFingerprint("FP2")
    // Define the vector that will contain the fingerprint
    val mol1 = new OBMol
    c.ReadString(mol1, molecule)

    val vfpcomp = new vectorUnsignedInt()
    val vfp = new vectorUnsignedInt()
    // Compute the fingerprint for each molecule in the input file and return the vfp which is a vectorUnsignedInt
    fingerprinter.GetFingerprint(mol1, vfpcomp)
    fingerprinter.GetFingerprint(mol, vfp)

    val tanimoto = OBFingerprint.Tanimoto(vfpcomp, vfp)



    return tanimoto

  }
//return compareFingerPrint("test", "CCCCCC")
//}

  def main(args: Array[String]) {
    // cp /usr/lib/jvm/java-1.7.0-openjdk/jre/lib/amd64/xawt/libmawt.so dans /usr/lib/jvm/java-1.7.0-openjdk/jre/lib/amd64/

    // Load OpenBabel Library
    System.loadLibrary("openbabel_java")
    // Define the InputFile containing smiles code
    val logFile = "/Users/christophebovigny/awesome/SparkOpenBabel/src/main/resources/README.md" // Should be some file on your system
   // Define the name of application and the conf for spark programm
    val conf = new SparkConf().setAppName("Simple Application")
    // Here the conf is set to overwrite the output if already exist
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    // Create a new instance of Spark context
    val sc = new SparkContext(conf)
    val molecule = "CCCCCCC"

    // We put all the files in the RAM Memory : and we split in 8 processes
    val logData = sc.textFile(logFile, 8).cache()
    // Here we compute the number of Carbon in each smiles molecule
    val numAs = logData.filter(line => line.contains("C")).count()
    // Here we compute the number of Oxygen in each smiles molecule
    val numBs = logData.filter(line => line.contains("O")).count()
    // Println the number of Carbon and the number of oxygens in input files
    println("Lines with C: %s, Lines with O: %s".format(numAs, numBs))

    // Save Vector FingerPrint FP2 in one file (coalesce(1))
    // Be careful, not sure that is the best way to do in term of computation. Coalesce could be really slow.
   // logData.map(computeFP2).map({ x=>  for(i <- 0 to 31) yield (x.get(i).toString)}).coalesce(1).saveAsTextFile("/Users/christophebovigny/awesome/SparkOpenBabel/src/main/resources/out")
    logData.map(x=> compareFingerPrint(x,molecule)).coalesce(1).saveAsTextFile("/Users/christophebovigny/awesome/SparkOpenBabel/src/main/resources/out")









  }
}
