
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import spark.*;

public class PartitionConfigurationTest {

//    @Test
//    void cellSizeAndBufferTest() {
//
//        PartitionConfiguration cfg =
//                new PartitionConfiguration(
//                        0.1f,
//                        0.75f,
//                        0.1f,
//                        0.8f,
//                        0.1f,
//                        3f,
//                        1f
//                );
//
//
//        assertEquals(0.3f, cfg.cellSize, 1e-6);
//        assertEquals(0.1f, cfg.buffer, 1e-6);
//    }
//
//    @Test
//    void numCellsTest() {
//
//        PartitionConfiguration cfg =
//                new PartitionConfiguration(
//                        0.1f,
//                        0.75f,
//                        0.1f,
//                        0.8f,
//                        0.1f,
//                        3f,
//                        1f
//                );
//
//
//        assertEquals(3, cfg.numCellsX);
//
//
//        assertEquals(3, cfg.numCellsY);
//    }
//
//
//    @Test
//    void print_cell_ranges_for_debugging() {
//
//        PartitionConfiguration cfg =
//                new PartitionConfiguration(
//                        0.1f,
//                        0.75f,
//                        0.1f,
//                        0.8f,
//                        0.1f,
//                        3f,
//                        1f
//                );
//
//        System.out.println("Partition Configuration: ");
//        System.out.println("cellSize = " + cfg.cellSize);
//        System.out.println("buffer   = " + cfg.buffer);
//        System.out.println("numCellsX = " + cfg.numCellsX);
//        System.out.println("numCellsY = " + cfg.numCellsY);
//        System.out.println();
//
//        System.out.println("X Cell Ranges:");
//        for (int x = 0; x < cfg.numCellsX; x++) {
//            float start = cfg.minLatitude + x * cfg.cellSize;
//            float end   = start + cfg.cellSize;
//            System.out.printf("cellX %d : [%.2f , %.2f)%n", x, start, end);
//        }
//
//        System.out.println();
//        System.out.println("Y Cell Ranges:");
//        for (int y = 0; y < cfg.numCellsY; y++) {
//            float start = cfg.minLongitude + y * cfg.cellSize;
//            float end   = start + cfg.cellSize;
//            System.out.printf("cellY %d : [%.2f , %.2f)%n", y, start, end);
//        }
//
//        System.out.println();
//        System.out.println("CellId Mapping (cellId = y * numCellsX + x)");
//        for (int y = 0; y < cfg.numCellsY; y++) {
//            for (int x = 0; x < cfg.numCellsX; x++) {
//                int cellId = y * cfg.numCellsX + x;
//                System.out.printf("cellId %d -> (x=%d, y=%d)%n", cellId, x, y);
//            }
//        }
//    }


}
