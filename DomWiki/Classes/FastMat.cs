internal static class FastMath
{

    unsafe public static float fastInvSqrt(float x) // = 1/sqrt(x)
    {
        int i = *(int*)&x;
        i = 0x5f3759df - (i >> 1);
        float y = *(float*)&i;
        return y * (1.5F - 0.5F * x * y * y);
    }

    unsafe public static double fastInvSqrt(double x) // = 1/sqrt(x)
    {
        long i = *(long*)&x;
        i = 0x5FE6EB50C7B537A9 - (i >> 1);
        double y = *(double*)&i;
        return y * (1.5D - 0.5D * x * y * y);
    }

    unsafe public static bool fastCompare(byte[] a1, byte[] a2) {
    // Copyright (c) 2008-2013 Hafthor Stefansson
    // Distributed under the MIT/X11 software license
    // Ref: http://www.opensource.org/licenses/mit-license.php.
        if (a1 == null || a2 == null || a1.Length != a2.Length) return false;
        fixed (byte* p1 = a1, p2 = a2) {
            byte* x1 = p1, x2 = p2;
            int l = a1.Length;
            for (int i = 0; i < l / 8; i++, x1 += 8, x2 += 8)
                if (*((long*) x1) != *((long*) x2))
                    return false;
            if ((l & 4) != 0) {
                if (*((int*) x1) != *((int*) x2)) return false;
                x1 += 4;
                x2 += 4;
            }
            if ((l & 2) != 0) {
                if (*((short*) x1) != *((short*) x2)) return false;
                x1 += 2;
                x2 += 2;
            }
            if ((l & 1) != 0)
                if (*((byte*) x1) != *((byte*) x2))
                    return false;
            return true;
        }
    }

    
    unsafe public static void fastCopy(byte[] from, byte[] to, int count) {
    // Modification of fastCompare
    // Distributed under the MIT/X11 software license
    // Ref: http://www.opensource.org/licenses/mit-license.php.
        if (from == null || to == null) return ;
        if (count > from.Length) count = from.Length;
        if (count > to.Length) count = to.Length;
        fixed (byte* p1 = from, p2 = to) {
            byte* x1 = p1, x2 = p2;
            int l = from.Length;
            for (int i = 0; i < l / 8; i++, x1 += 8, x2 += 8)
                *((long*) x2) = *((long*) x1);
            if ((l & 4) != 0) {
                *((int*) x2) = *((int*) x1);
                x1 += 4;
                x2 += 4;
            }
            if ((l & 2) != 0) {
                *((short*) x2) = *((short*) x1);
                x1 += 2;
                x2 += 2;
            }
            if ((l & 1) != 0)
                *((byte*) x2) = *((byte*) x1);
        }
    }
}