import org.junit.Assert;
import org.junit.Test;

public class test {
    @Test
    public void ValidateCount1() {
        Assert.assertEquals(4696,usecase1.getCount());
    }
    @Test
    public void ValidateCount2() {
        Assert.assertEquals(0,usecase2.getCount());
    }
    @Test
    public void ValidateCount3() {
        Assert.assertEquals(1941,usecase3.getCount());
    }
    @Test
    public void ValidateCount4() {
        Assert.assertEquals(33,usecase4.getCount());
    }
    @Test
    public void ValidateCount5() {
        Assert.assertEquals(6,usecase5.getCount());
    }

}
