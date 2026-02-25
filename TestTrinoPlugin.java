import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;

public class TestTrinoPlugin {
  public static void main(String[] args) {
    try {
      IDatabase trino = DatabaseMeta.getIDatabase("TRINO");
      if (trino != null) {
        System.out.println("✓ Trino plugin found successfully!");
        System.out.println("  Type: " + trino.getPluginId());
        System.out.println("  Name: " + trino.getPluginName());
        System.out.println("  Is Trino Variant: " + trino.isTrinoVariant());
        System.out.println("  Driver: " + trino.getDriverClass());
      } else {
        System.out.println("✗ Trino plugin not found!");
      }
    } catch (Exception e) {
      System.out.println("✗ Error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}

