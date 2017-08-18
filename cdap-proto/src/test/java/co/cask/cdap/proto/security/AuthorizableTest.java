package co.cask.cdap.proto.security;

import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Authorizable}
 */
public class AuthorizableTest {

  @Test
  public void testFromEntity() {
    ProgramId programId = new ProgramId("ns", "app", ProgramType.MAPREDUCE, "prog");
    Authorizable authorizable = Authorizable.fromEntityId(programId);
    Assert.assertEquals(programId.toString().replace(ApplicationId.DEFAULT_VERSION + ".", ""), authorizable.toString());
  }
}
