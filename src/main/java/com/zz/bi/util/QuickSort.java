package com.zz.bi.util;

public class QuickSort {
    public static void main(String[] args) {
        int[] a = {10, 9, 2, 3, 7, 9, 23, 21, 62, 9, 7, 23, 12, 98, 123, 64, 129, 119};
        sort(a, 0, a.length - 1);
        for (int x : a) {
            System.out.println(x);
        }

    }

    private static void sort(int[] arr, int left, int right) {
        if (left >= right) return;
        int i = left; //i 为左侧探针，用于探测大于基准数的位置
        int j = right;//j 为右侧探针，用于探测小于基准数的位置
        int flag = arr[i]; //选取左侧第一个数作为基准数
        while (i < j) {
            //从右往左找到第一个小于基准数的坐标
            while (i < j && arr[j] >= flag) {
                j--;
            }
            //从左往右找到第一个大于基准数的坐标
            while (i < j && arr[i] <= flag) {
                i++;
            }
            // i指向的是大于基准数的位置，j指向的是小于基准数的位置;
            // 因为基准数左侧都是小于基准数的数，右侧都是大于基准数的数，所以要交换两个数的位置
            if (i < j) {
                swap(arr, i, j);
            }
        }
        swap(arr, left, i);//此时i的位置是本轮循环两个探针同时指向的位置,此处应该放置基准数，因此要跟最左侧数字交换位置。
        //本轮循环完成后，数组被分为两部分，要分别对基准数左侧和右侧的数进行排序。
        sort(arr, left, i - 1);
        sort(arr, i + 1, right);
    }

    /**
     * 交换数组arr中left位置和right位置的元素
     *
     * @param arr
     * @param left
     * @param right
     */
    private static void swap(int[] arr, int left, int right) {
        int tmp = arr[left];
        arr[left] = arr[right];
        arr[right] = tmp;
    }
}
