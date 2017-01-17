public final class Primitives {
    private Primitives () {
    };
    public static native double incircle(double[] pa, double[] pb, double[] pc, double[] pd);
    public static native double orient2d(double[] pa, double[] pb, double[] pc);
    public static native double[] computeCircumcenter(double[] pa, double[] pb, double[] pc);
    
    static {
        System.loadLibrary("CPrimitives");
    }
}    
