package org.curieo.utils;

import java.util.Collections;
import java.util.List;
import lombok.Generated;
import lombok.Value;
import org.junit.jupiter.api.Test;

class StringUtilsTest {

  @Generated
  @Value
  static class TestCase {
    String text;
    List<String> emails;
  }

  @Test
  void testEmail() {
    TestCase[] testCases =
        new TestCase[] {
          new TestCase("", Collections.emptyList()),
          new TestCase("Department of Food Science, Hena.", Collections.emptyList()),
          new TestCase(
              "Laboratorio de Biotón en Alimeno. mazorra@ciad.mx", Collections.emptyList()),
          new TestCase(
              "Department of Research and Deve58, Japan. c-ito@fujicco.co.jp",
              Collections.emptyList()),
          new TestCase(
              "Department of Research and Deve58, Japan (c-ito@fujicco.co.jp).",
              Collections.emptyList()),
          new TestCase(
              "Department of Food Science and 50011, USA. vkapchie@gmail.com",
              Collections.emptyList()),
          new TestCase(
              "INSERM, UMR 1100, Pathologies Respiratoires: protéolyse et aérosolthérapie, Centre d'Etude des Pathologies Respiratoires, Université François Rabelais, F-37032 Tours cedex, France. Electronic address: fabien.lecaille@univ-tours.fr.",
              Collections.emptyList()),
          new TestCase(
              "Center for Cancer Prevention and Drug Development, 975 NE 10th Street, BRC 1203, University of Oklahoma Health Sciences Center, Oklahoma City, OK 73104. cv-rao@ouhsc.edu; and Jagan M.R. Patlolla, E-mail: Jagan-Patlolla@ouhsc.edu.",
              Collections.emptyList()),
          new TestCase(
              "Department of Biomedical Science, Mercer University School of Medicine, Savannah Campus, 4700 Waters Ave, Savannah, GA 31404-3089, USA. Jiang_s@mercer.edu.",
              Collections.emptyList())
        };
    for (TestCase testCase : testCases) {
      List<String> emails = StringUtils.extractEmails(testCase.getText());
      System.out.println(String.join(", ", emails));
    }
  }
}
